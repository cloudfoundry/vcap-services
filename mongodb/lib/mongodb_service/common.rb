# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require "client"

module VCAP
  module Services
    module MongoDB
      module Common
        def service_name
          "MongoaaS"
        end
        class ProvisionedService
        end
      end
    end
  end
end

class VCAP::Services::MongoDB::Common::ProvisionedService
  include DataMapper::Resource
  property :name,       String,   :key => true
  property :port,       Integer,  :unique => true
  property :password,   String,   :required => true
  property :plan,       Enum[:free], :required => true
  property :pid,        Integer
  property :memory,     Integer
  property :admin,      String,   :required => true
  property :adminpass,  String,   :required => true
  property :db,         String,   :required => true
  property :container,  String
  property :ip,         String

  private_class_method :new

  # Timeout for mongo client operations, node cannot be blocked on any mongo instances.                                                                                                                                                     
  # Default value is 2 seconds                                                                                                                                                                                                              
  MONGO_TIMEOUT = 2

  class << self
    def init(args)
      raise "Parameter missing" unless args[:base_dir] && args[:local_db]
      @@mongorestore_path = args[:mongorestore_path] ? args[:mongorestore_path] : 'mongorestore'
      @@mongodump_path    = args[:mongodump_path] ? args[:mongodump_path] : 'mongodump'
      @@tar_path          = args[:tar_path] ? args[:tar_path] : 'tar'
      @@base_dir          = args[:base_dir]
      FileUtils.mkdir_p(@@base_dir)
      @@warden_client     = Warden::Client.new("/tmp/warden.sock")
      @@warden_client.connect
      DataMapper.setup(:default, args[:local_db])
      DataMapper::auto_upgrade!
    end

    def create(args)
      raise "Parameter missing" unless args['port']
      p_service           = new
      p_service.name      = args['name'] ? args['name'] : UUIDTools::UUID.random_create.to_s
      p_service.port      = args['port']
      p_service.plan      = args['plan'] ? args['plan'] : 'free'
      p_service.password  = args['password'] ? args['password'] : UUIDTools::UUID.random_create.to_s
      p_service.memory    = args['memory'] if args['memory']
      p_service.admin     = args['admin'] ? args['admin'] : 'admin'
      p_service.adminpass = args['adminpass'] ? args['adminpass'] : UUIDTools::UUID.random_create.to_s
      p_service.db        = args['db'] ? args['db'] : 'db'
      
      raise "Cannot save provision service" unless p_service.save!

      FileUtils.rm_rf(p_service.service_dir)
      FileUtils.mkdir_p(File.join(p_service.service_dir, 'data'))
      FileUtils.mkdir_p(File.join(p_service.service_dir, 'log'))
      p_service
    end

    def import(port, dir)
      d_file = File.join(dir, 'dump_file')
      raise "No dumpfile exists" unless File.exist?(d_file)

      s_service = nil
      File.open(d_file, 'r') do |f|
        s_service = Marshal.load(f)
      end
      raise "Cannot parse dumpfile in #{d_file}" if s_service.nil?

      p_service = create('name'      => s_service.name,
                         'port'      => port,
                         'plan'      => s_service.plan,
                         'password'  => s_service.password,
                         'memory'    => s_service.memory,
                         'admin'     => s_service.admin,
                         'adminpass' => s_service.adminpass,
                         'db'        => s_service.db)
      FileUtils.cp_r(File.join(dir, 'data'), p_service.service_dir)
      FileUtils.cp_r(File.join(dir, 'log'), p_service.service_dir)
      p_service
    end
  end

  def delete
    # stop container
    stop if running?
    # delete log and service directory
    FileUtils.rm_rf(service_dir)
    # delete recorder
    destroy!
  end

  def dump(dir)
    # dump database recorder
    d_file = File.join(dir, 'dump_file')
    File.open(d_file, 'w') do |f|
      Marshal.dump(self, f)
    end
    # dump database data/log directory
    FileUtils.cp_r(File.join(service_dir, 'data'), dir)
    FileUtils.cp_r(File.join(service_dir, 'log'), dir)
  end

  def d_import(dir)
    conn = connect
    db = conn.db(self[:db])
    db.collection_names.each do |name|
      if name != 'system.users' && name != 'system.indexes'
        db[name].drop
      end
    end
    disconnect(conn)

    output = %x{ #{@@mongorestore_path} -u #{self[:admin]} -p #{self[:adminpass]} -h #{self[:ip]}:27017 #{dir} }
    res = $?.success?
    raise "\"#{cmd}\" failed" unless res
    true
  end

  def d_dump(dir, fake=true)
    cmd = "#{@@mongodump_path} -u #{self[:admin]} -p #{self[:adminpass]} -h #{self[:ip]}:27017 -o #{dir}"
    return cmd if fake
    output = %x{ #{@@mongodump_path} -u #{self[:admin]} -p #{self[:adminpass]} -h #{self[:ip]}:27017 -o #{dir} }
    res = $?.success?
    raise "\"#{cmd}\" failed" unless res
    true
  end

  # mongod control 
  def running?
    if (self[:container] == '')
      return false
    else
      @@warden_client.call(["info", self[:container]])
      return true
    end
  rescue => e
    return false
  end

  def stop(timeout=5,sig=:SIGTERM)
    unmapping_port(self[:ip], self[:port])
    @@warden_client.call(["stop", self[:container]])
    @@warden_client.call(["destroy", self[:container]])
    self[:container] = ''
    save
    true
  end

  def run
    req = ["create", {"bind_mounts" => [[service_dir, "/store", {"mode" => "rw"}]]}]
    self[:container] = @@warden_client.call(req)
    a = self[:container].hex
    b = ["#{(a/0x1000000)%0x100}", "#{(a/0x10000)%0x100}", "#{(a/0x100)%0x100}", "#{(a%0x100)+2}"]
    self[:ip] = b.join(".")
    save!
    mapping_port(self[:ip], self[:port])
  end

  def mapping_port(ip, port)
    rule = [ "--protocol tcp",
             "--dport #{port}",
             "--jump DNAT",
             "--to-destination #{ip}:27017" ]
    puts "iptables -t nat -A PREROUTING #{rule.join(" ")}"
    system "iptables -t nat -A PREROUTING #{rule.join(" ")}"
  end

  def unmapping_port(ip, port)
    rule = [ "--protocol tcp",
             "--dport #{port}",
             "--jump DNAT",
             "--to-destination #{ip}:27017" ]
    puts "iptables -t nat -D PREROUTING #{rule.join(" ")}"
    system "iptables -t nat -D PREROUTING #{rule.join(" ")}"
  end

  # diretory helper
  def service_dir
    File.join(@@base_dir, self[:name])
  end

  def service_dir?
    Dir.exists?(service_dir)
  end

  # user management helper
  def add_admin(username, password)
    @@warden_client.call(["run", self[:container], "mongo localhost:27017/admin --eval 'db.addUser(\"#{username}\", \"#{password}\")'"])
  rescue => e
    raise "Could not add admin user \'#{username}\'"
  end

  def add_user(username, password)
    conn = connect
    Timeout::timeout(MONGO_TIMEOUT) do
      conn.db(self[:db]).add_user(username, password)
    end
    disconnect(conn)
  end

  def remove_user(username)
    conn = connect
    Timeout::timeout(MONGO_TIMEOUT) do
      conn.db(self[:db]).remove_user(username)
    end
    disconnect(conn)
  end

  # mongodb connection
  def connect
    conn = nil
    return conn unless running?
    Timeout::timeout(MONGO_TIMEOUT) do
      conn = Mongo::Connection.new(self[:ip], '27017')
      auth = conn.db('admin').authenticate(self[:admin], self[:adminpass])
      raise "Authentication failed, instance: #{self[:name]}" unless auth
    end
    conn
  end
  
  def disconnect(conn)
    conn.close if conn
  end

  # stats helpers
  def overall_stats
    st = nil
    conn = connect
    Timeout::timeout(MONGO_TIMEOUT) do
      st = conn.db('admin').command(:serverStatus => 1)
    end
    disconnect(conn)
    return st
  rescue => e
    "Failed mongodb_overall_stats: #{e.message}, instance: #{self[:name]}"
  end

  def db_stats
    st = nil
    conn = connect
    Timeout::timeout(MONGO_TIMEOUT) do
      st = conn.db(self[:db]).stats()
    end
    disconnect(conn)
    return st
  rescue => e
    "Failed mongodb_db_stats: #{e.message}, instance: #{self[:name]}"
  end

  def get_healthz
    conn = connect
    disconnect(conn)
    "ok"
  rescue => e
    "fail"
  end

end
