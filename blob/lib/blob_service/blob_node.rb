# Copyright (c) 2009-2011 VMware, Inc.
require "erb"
require "fileutils"
require "logger"
require "pp"
require "set"
require "mongo"
require "timeout"
require "net/http"

require "datamapper"
require "nats/client"
require "uuidtools"

require 'vcap/common'
require 'vcap/component'
require "blob_service/common"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..', 'base', 'lib')
require 'base/node'

module VCAP
  module Services
    module Blob
      class Node < VCAP::Services::Base::Node
      end
    end
  end
end

class VCAP::Services::Blob::Node

  include VCAP::Services::Blob::Common

  # FIXME only support rw currently
  BIND_OPT = 'rw'

  class ProvisionedService
    include DataMapper::Resource
    property :name,         String,   :key => true
    property :port,         Integer,  :unique => true
    property :meta_port,    Integer,  :unique => true
    property :password,     String,   :required => true
    property :plan,         Enum[:free], :required => true
    property :pid,          Integer
    property :meta_pid,     Integer
    property :memory,       Integer
    property :meta_memory,  Integer
    property :admin,        String,   :required => true
    property :adminpass,    String,   :required => true
    property :keyid,        String,   :required => true
    property :secretid,     String,   :required => true
    property :db,           String,   :required => true

    def listening?
      begin
        TCPSocket.open('localhost', port).close
        return true
      rescue => e
        return false
      end
    end

    def meta_listening?
      begin
        TCPSocket.open('localhost', meta_port).close
        return true
      rescue => e
        return false
      end
    end

    def running?
      return false unless !pid.nil?
      VCAP.process_running? pid
    end

    def meta_running?
      return false unless !meta_pid.nil?
      VCAP.process_running? meta_pid
    end

    def kill(sig=:SIGTERM)
      if !pid.nil?
        @wait_thread = Process.detach(pid)
        Process.kill(sig, pid) if running?
      end
      if !meta_pid.nil?
        @wait_thread_meta = Process.detach(meta_pid)
        Process.kill(sig,meta_pid) if meta_running?
      end
    end

    def wait_killed(timeout=5, interval=0.2)
      begin
        Timeout::timeout(timeout) do
          @wait_thread.join if @wait_thread
          @wait_thread_meta.join if @wait_thread_meta
          while running? || meta_running? do
            sleep interval
          end
        end
      rescue Timeout::Error
        return false
      end
      true
    end
  end

  def initialize(options)
    super(options)
    @base_dir = options[:base_dir]
    FileUtils.mkdir_p(@base_dir)
    @nodejs_path = options[:nodejs_path]
    @blobd_path = options[:blobd_path]
    @blobrestore_path = options[:blobrestore_path]
    @metad_path = options[:metad_path]
    @metarestore_path = options[:metarestore_path]
    @total_memory = options[:available_memory]
    @available_memory = options[:available_memory]
    @max_memory = options[:max_memory]
    @meta_max_memory = options[:meta_max_memory]

    @config_template = ERB.new(File.read(options[:config_template]))
    @meta_config_template = ERB.new(File.read(options[:meta_config_template]))

    DataMapper.setup(:default, options[:local_db])
    DataMapper::auto_upgrade!

    @free_ports = Set.new
    options[:port_range].each {|port| @free_ports << port}
  end

  # start blob instance and its meta instance, meta first, then blob service!
  def pre_send_announcement
    ProvisionedService.all.each do |provisioned_service|
      @free_ports.delete(provisioned_service.port)
      @free_ports.delete(provisioned_service.meta_port)
      if provisioned_service.meta_listening?
        @logger.info("Service #{provisioned_service.name} already listening on meta_port #{provisioned_service.meta_port}")
        @available_memory -= (provisioned_service.meta_memory || @meta_max_memory)
      else
        begin
          meta_pid = start_instance_meta(provisioned_service)
          provisioned_service.meta_pid = meta_pid
          unless provisioned_service.save
            provisioned_service.kill
            raise "Couldn't save pid (#{pid})"
          end
        rescue => e
          @logger.warn("Error starting service meta #{provisioned_service.name}: #{e}")
        end
      end
      if provisioned_service.listening?
        @logger.info("Service #{provisioned_service.name} already listening on port #{provisioned_service.port}")
        @available_memory -= (provisioned_service.memory || @max_memory)
      else
        begin
          pid = start_instance(provisioned_service)
          provisioned_service.pid = pid
          unless provisioned_service.save
            provisioned_service.kill
            raise "Couldn't save pid (#{pid})"
          end
        rescue => e
          @logger.warn("Error starting service #{provisioned_service.name}: #{e}")
        end
      end
    end
  end

  def shutdown
    super
    @logger.info("Shutting down instances..")
    ProvisionedService.all.each { |provisioned_service|
      @logger.debug("Trying to terminate blobd pid:#{provisioned_service.pid} and meta pid:#{provisioned_service.meta_pid}")
      provisioned_service.kill(:SIGTERM)
      provisioned_service.wait_killed ?
      @logger.debug("Blobd pid:#{provisioned_service.pid} and meta pid:#{provisioned_service.meta_pid} terminated") :
        @logger.warn("Timeout to terminate blobd pid:#{provisioned_service.pid} and meta pid:#{provisioned_service.meta_pid}")
    }
  end

  def announcement
    a = {
      :available_memory => @available_memory
    }
    a
  end


  def provision(plan, credential = nil)
    port   = credential && credential['port'] && @free_ports.include?(credential['port']) ? credential['port'] : @free_ports.first
    @free_ports.delete(port)
    name   = credential && credential['name'] ? credential['name'] : UUIDTools::UUID.random_create.to_s
    db     = credential && credential['db']   ? credential['db']   : 'db'
    meta_port  = credential && credential['meta_port'] && @free_ports.include?(credential['meta_port']) ? credential['meta_port'] : @free_ports.first
    @free_ports.delete(meta_port)

    username = credential && credential['username'] ? credential['username'] : UUIDTools::UUID.random_create.to_s
    password = credential && credential['password'] ? credential['password'] : UUIDTools::UUID.random_create.to_s

    provisioned_service             = ProvisionedService.new
    provisioned_service.name        = name
    provisioned_service.port        = port
    provisioned_service.meta_port   = meta_port
    provisioned_service.plan        = plan
    provisioned_service.password    = UUIDTools::UUID.random_create.to_s
    provisioned_service.memory      = @max_memory
    provisioned_service.meta_memory = @meta_max_memory
    provisioned_service.meta_pid    = start_instance_meta(provisioned_service)
    provisioned_service.admin       = 'admin'
    provisioned_service.adminpass   = UUIDTools::UUID.random_create.to_s
    provisioned_service.keyid       = username
    provisioned_service.secretid    = password
    provisioned_service.db          = db

    unless provisioned_service.save
      cleanup_service(provisioned_service)
      raise "Could not save entry: #{provisioned_service.errors.pretty_inspect}"
    end

    begin
      # wait for meta to start
      sleep 0.5

      blob_add_admin({
        :port      => provisioned_service.meta_port,
        :username  => provisioned_service.admin,
        :password  => provisioned_service.adminpass,
        :times     => 10
      })
      blob_add_user({
        :port      => provisioned_service.meta_port,
        :admin     => provisioned_service.admin,
        :adminpass => provisioned_service.adminpass,
        :db        => provisioned_service.db,
        :username  => provisioned_service.admin,
        :password  => provisioned_service.adminpass
      })
    rescue => e
      record_service_log(provisioned_service.name)
      cleanup_service(provisioned_service)
      raise e.to_s + ": Could not save admin user."
    end

    provisioned_service.pid = start_instance(provisioned_service)

    unless provisioned_service.save
      cleanup_service(provisioned_service)
      raise "Could not save entry: #{provisioned_service.errors.pretty_inspect}"
    end

    response = {
      "host" => @local_ip,
      "port" => provisioned_service.port,
      "name" => provisioned_service.name,
      "username" => username,
      "password" => password
    }
    @logger.debug("response: #{response}")
    return response
  end

  def unprovision(name, bindings)
    provisioned_service = ProvisionedService.get(name)
    raise ServiceError.new(ServiceError::NOT_FOUND, name) if provisioned_service.nil?
    cleanup_service(provisioned_service)
    @logger.debug("Successfully fulfilled unprovision request: #{name}.")
    true
  end

  def cleanup_service(provisioned_service)
    @logger.debug("Killing #{provisioned_service.name} started with pid #{provisioned_service.pid} and with meta_pid #{provisioned_service.meta_pid}")
    raise "Could not cleanup service: #{provisioned_service.errors.pretty_inspect}" unless provisioned_service.destroy
    provisioned_service.kill(:SIGKILL) if provisioned_service.running? || provisioned_service.meta_running?
    dir = File.join(@base_dir, provisioned_service.name)
    EM.defer { FileUtils.rm_rf(dir) }
    # TODO: delete mongo log which was in another folder
    @available_memory += provisioned_service.memory
    @available_memory += provisioned_service.meta_memory
    @free_ports << provisioned_service.port
    @free_ports << provisioned_service.meta_port
    true
  end

  # provid the key/serect to blob gw
  def bind(name, bind_opts, credential = nil)
    @logger.debug("Bind request: name=#{name}, bind_opts=#{bind_opts}")
    bind_opts ||= BIND_OPT
    provisioned_service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if provisioned_service.nil?
    response = {
      "host" => @local_ip,
      "port"     => provisioned_service.port,
      "username" => provisioned_service.keyid,
      "password" => provisioned_service.secretid,
      "name"     => provisioned_service.name,
    }
    @logger.debug("response: #{response}")
    response
  end

  # no need to do anything
  def unbind(credential)
    @logger.debug("Unbind request: credential=#{credential}")
    name = credential['name']
    provisioned_service = ProvisionedService.get(name)
    raise ServiceError.new(ServiceError::NOT_FOUND, name) if provisioned_service.nil?
    @logger.debug("Successfully unbind #{credential}")
    true
  end

  def varz_details
    # Do disk summary
    du_hash = {}
    du_all_out = `cd #{@base_dir}; du -sk * 2> /dev/null`
    du_entries = du_all_out.split("\n")
    du_entries.each do |du_entry|
      size, dir = du_entry.split("\t")
      size = size.to_i * 1024 # Convert to bytes
      du_hash[dir] = size
    end

    # Get meta db.stats 
    stats = []
    ProvisionedService.all.each do |provisioned_service|
      stat = {}
      db_stats = blob_db_stats({
        :port      => provisioned_service.meta_port,
        :name      => provisioned_service.name,
        :admin     => provisioned_service.admin,
        :adminpass => provisioned_service.adminpass,
        :db        => provisioned_service.db
      })
      stat['db'] = db_stats
      stat['name'] = provisioned_service.name
      stats << stat
    end
    {
      :running_services     => stats,
      :disk                 => du_hash,
      :services_max_memory  => @total_memory,
      :services_used_memory => @total_memory - @available_memory
    }
  end

  def healthz_details
    healthz = {}
    healthz[:self] = "ok"
    ProvisionedService.all.each do |instance|
      healthz[instance.name.to_sym] = get_healthz(instance)
    end
    healthz
  rescue => e
    @logger.warn("Error get healthz details: #{e}")
    {:self => "fail"}
  end

  def get_healthz(instance)
    conn = Mongo::Connection.new(@local_ip, instance.meta_port)
    auth = conn.db('admin').authenticate(instance.admin, instance.adminpass)

    # ping blob gw
    req = Net::HTTP::Get.new("/")
    res = Net::HTTP.start(@local_ip, instance.port) {|http|
      http.request(req)
    }
    auth && res && res.code == "401" ? "ok" : "fail"
  rescue => e
    @logger.warn("Getting healthz for #{instance.inspect} failed with error #{e}")
    "fail"
  ensure
    conn.close if conn
  end

  def start_instance(provisioned_service)
    @logger.debug("Starting: #{provisioned_service.pretty_inspect}")
    memory = @max_memory
    pid = fork
    if pid
      @logger.debug("Service #{provisioned_service.name} started with pid #{pid}")
      @available_memory -= memory
      # In parent, detach the child.
      Process.detach(pid)
      pid
    else
      $0 = "Starting Blob service: #{provisioned_service.name}"
      close_fds
      blob_port = provisioned_service.port
      dir = service_dir(provisioned_service.name)
      blob_dir = blob_dir(dir)
      FileUtils.mkdir_p(blob_dir);
      default_driver = "fs-#{provisioned_service.name}"
      drivers = [{"fs-#{provisioned_service.name}" =>
        {"type"=>"fs",
          "option" => {
            "root" => blob_dir,
            "mds" => {
              "host"=>"127.0.0.1",
              "port"=>provisioned_service.meta_port,
              "db"=>provisioned_service.db,
              "user"=>provisioned_service.admin,
              "pwd"=>provisioned_service.adminpass
            }
          }
        }}];
      log_file = log_file_blob(dir)
      keyid = provisioned_service.keyid
      secretid = provisioned_service.secretid
      config = @config_template.result(binding)
      config_path = File.join(dir, "config.json")
      FileUtils.rm_f(log_file)
      FileUtils.rm_f(config_path)
      File.open(config_path, "w") {|f| f.write(config.gsub("=>",":"))} #changing to js compatible format
      cmd = "#{@nodejs_path} #{@blobd_path} -f #{config_path}"
      exec(cmd) rescue @logger.warn("exec(#{cmd}) failed!")
    end
  end

  def start_instance_meta(provisioned_service)
    @logger.debug("Starting: #{provisioned_service.pretty_inspect}")
    memory = @meta_max_memory
    pid = fork
    if pid
      @logger.debug("Service #{provisioned_service.name} started meta with meta pid #{pid}")
      @available_memory -= memory
      # In parent, detach the child.
      Process.detach(pid)
      pid
    else
      $0 = "Starting Blob meta service: #{provisioned_service.name}"
      close_fds
      port = provisioned_service.meta_port
      password = provisioned_service.password
      dir = service_dir(provisioned_service.name)
      meta_data_dir = meta_data_dir(dir)
      log_file = log_file(dir)
      config = @meta_config_template.result(binding)
      config_path = File.join(dir, "mongo.conf")
      FileUtils.mkdir_p(dir)
      FileUtils.mkdir_p(meta_data_dir)
      FileUtils.rm_f(log_file)
      FileUtils.rm_f(config_path)
      File.open(config_path, "w") {|f| f.write(config)}
      cmd = "#{@metad_path} -f #{config_path}"
      exec(cmd) rescue @logger.warn("exec(#{cmd}) failed!")
    end
  end

  def memory_for_service(provisioned_service)
    case provisioned_service.plan
      when :free then 256
      else
        raise "Invalid plan: #{provisioned_service.plan}"
    end
  end

  def close_fds
    3.upto(get_max_open_fd) do |fd|
      begin
        IO.for_fd(fd, "r").close
      rescue
      end
    end
  end

  def get_max_open_fd
    max = 0

    dir = nil
    if File.directory?("/proc/self/fd/") # Linux
      dir = "/proc/self/fd/"
    elsif File.directory?("/dev/fd/") # Mac
      dir = "/dev/fd/"
    end

    if dir
      Dir.foreach(dir) do |entry|
        begin
          pid = Integer(entry)
          max = pid if pid > max
        rescue
        end
      end
    else
      max = 65535
    end

    max
  end

  def blob_add_admin(options)
    @logger.info("add admin user: req #{options}")
    t = options[:times] || 1
    conn = nil

   t.times do
      begin
        conn = Mongo::Connection.new('127.0.0.1', options[:port])
        user = conn.db('admin').add_user(options[:username], options[:password])
        raise "user not added" if user.nil?
        @logger.debug("user #{options[:username]} added in db #{options[:db]}")
        return true
      rescue => e
        @logger.warn("Failed add user #{options[:username]}: #{e.message}")
        sleep 1
      end
    end

    raise "Could not add admin user #{options[:username]}"
  ensure
    conn.close if conn
  end

  def blob_add_user(options)
    @logger.debug("add user in port: #{options[:port]}, db: #{options[:db]}")
    conn = Mongo::Connection.new('127.0.0.1', options[:port])
    auth = conn.db('admin').authenticate(options[:admin], options[:adminpass])
    db = conn.db(options[:db])
    db.add_user(options[:username], options[:password])
    @logger.debug("user #{options[:username]} added")
  ensure
    conn.close if conn
  end

  def blob_remove_user(options)
    @logger.debug("remove user in port: #{options[:port]}, db: #{options[:db]}")
    conn = Mongo::Connection.new('127.0.0.1', options[:port])
    db = conn.db(options[:db])
    auth = db.authenticate(options[:admin], options[:adminpass])
    db.remove_user(options[:username])
    @logger.debug("user #{options[:username]} removed")
  ensure
    conn.close if conn
  end

  def blob_overall_stats(options)
    conn = Mongo::Connection.new('127.0.0.1', options[:port])
    auth = conn.db('admin').authenticate(options[:admin], options[:adminpass])
    # The following command is not documented in mongodb's official doc.
    # But it works like calling db.serverStatus from client. And 10gen support has
    # confirmed it's safe to call it in such way.
    conn.db('admin').command({:serverStatus => 1})
  rescue => e
    @logger.warn("Failed blob_overall_stats: #{e.message}, options: #{options}")
    "Failed blob_overall_stats: #{e.message}, options: #{options}"
  ensure
    conn.close if conn
  end

  def blob_db_stats(options)
    conn = Mongo::Connection.new('127.0.0.1', options[:port])
    auth = conn.db(options[:db]).authenticate(options[:admin], options[:adminpass])
    conn.db(options[:db]).stats()
  rescue => e
    @logger.warn("Failed blob_db_stats: #{e.message}, options: #{options}")
    "Failed blob_db_stats: #{e.message}, options: #{options}"
  ensure
    conn.close if conn
  end

  def service_dir(service_id)
    File.join(@base_dir, service_id)
  end

  # TODO: replace with proper log location
  def log_file(base_dir)
    File.join(base_dir, 'log')
  end

  # TODO: replace with proper log location
  def log_file_blob(base_dir)
    File.join(base_dir, 'blob_log')
  end

  def meta_data_dir(base_dir)
    File.join(base_dir, 'meta_data')
  end

  def blob_dir(base_dir)
    File.join(base_dir,'blob_data')
  end

  def record_service_log(service_id)
    @logger.warn(" *** BEGIN blob log - instance: #{service_id}")
    base_dir = service_dir(service_id)
    file = File.new(log_file(base_dir), 'r')
    while (line = file.gets)
      @logger.warn(line.chomp!)
    end
    @logger.warn(" === ABOVE: meta log BELOW: blob gw log ===")
    base_dir = service_dir(service_id)
    file = File.new(log_file_blob(base_dir), 'r')
    while (line = file.gets)
      @logger.warn(line.chomp!)
    end
  rescue => e
    @logger.warn(e)
  ensure
    @logger.warn(" *** END blob log - instance: #{service_id}")
  end
end
