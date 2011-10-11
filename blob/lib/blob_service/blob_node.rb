# Copyright (c) 2009-2011 VMware, Inc.
require "erb"
require "fileutils"
require "logger"
require "pp"
require "set"
require "timeout"
require "net/http"
require "openssl"
require "digest/sha2"
require "base64"
require "yajl"
require "json"

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
    property :plan,         Enum[:free], :required => true
    property :pid,          Integer
    property :memory,       Integer
    property :keyid,        String,   :required => true
    property :secretid,     String,   :required => true

    def listening?
      begin
        TCPSocket.open('localhost', port).close
        return true
      rescue => e
        return false
      end
    end

    def running?
      return false unless !pid.nil?
      VCAP.process_running? pid
    end

    def kill(sig=:SIGTERM)
      if !pid.nil?
        @wait_thread = Process.detach(pid)
        Process.kill(sig, pid) if running?
      end
    end

    def wait_killed(timeout=5, interval=0.2)
      begin
        Timeout::timeout(timeout) do
          @wait_thread.join if @wait_thread
          while running? do
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
    @blobd_log_dir = options[:blobd_log_dir]
    @total_memory = options[:available_memory]
    @available_memory = options[:available_memory]
    @max_memory = options[:max_memory]

    @config_template = ERB.new(File.read(options[:config_template]))

    DataMapper.setup(:default, options[:local_db])
    DataMapper::auto_upgrade!

    @free_ports = Set.new
    options[:port_range].each {|port| @free_ports << port}
  end

  # start blob instance and its meta instance, meta first, then blob service!
  def pre_send_announcement
    ProvisionedService.all.each do |provisioned_service|
      @free_ports.delete(provisioned_service.port)
      if provisioned_service.listening?
        @logger.warn("Service #{provisioned_service.name} already listening on port #{provisioned_service.port}")
        @available_memory -= (provisioned_service.memory || @max_memory)
        next
      end

      unless service_exist?(provisioned_service)
        @logger.warn("Service #{provisioned_service.name} in local DB, but not in file system")
        next
      end

      begin
        pid = start_instance(provisioned_service)
        provisioned_service.pid = pid
        raise "Cannot save provision_service" unless provisioned_service.save
      rescue => e
        provisioned_service.kill
        @logger.error("Error starting service #{provisioned_service.name}: #{e}")
      end
    end
  end

  def shutdown
    super
    @logger.info("Shutting down instances..")
    ProvisionedService.all.each { |provisioned_service|
      @logger.debug("Trying to terminate blobd pid:#{provisioned_service.pid}")
      provisioned_service.kill(:SIGTERM)
      provisioned_service.wait_killed ?
        @logger.debug("Blobd pid:#{provisioned_service.pid} terminated") :
        @logger.error("Timeout to terminate blobd pid:#{provisioned_service.pid}")
    }
  end

  def announcement
    a = {
      :available_memory => @available_memory
    }
    a
  end

  def all_instances_list
    ProvisionedService.all.map{|ps| ps["name"]}
  end

  def all_bindings_list
    list = []
    ProvisionedService.all.each do |instance|
      begin
        # TODO:get all user names from ~admin bucket
        req = Net::HTTP::Get.new("/~bind")
        res = Net::HTTP.start(@local_ip, instance.port) {|http|
          http.request(req)
        }
        raise "Couldn't get binding list" if (!res || res.code != "200")
        bindings = Yajl::Parser.parse(msg)
        bindings.each_key {|key|
          credential = {
            'name' => instance.name,
            'port' => instance.port,
            'username' => key
          }
          list << credential if credential['username'] != instance.keyID
        }
      rescue => e
        @logger.warn("Failed fetch user list: #{e.message}")
      end
    end
    list
  end

  def provision(plan, credential = nil)
    @logger.debug("ProvisionProvision ")
    port   = credential && credential['port'] && @free_ports.include?(credential['port']) ? credential['port'] : @free_ports.first
    @free_ports.delete(port)
    name   = credential && credential['name'] ? credential['name'] : UUIDTools::UUID.random_create.to_s

    username = credential && credential['username'] ? credential['username'] : UUIDTools::UUID.random_create.to_s
    password = credential && credential['password'] ? credential['password'] : UUIDTools::UUID.random_create.to_s
    # Cleanup instance dir if it exists
    FileUtils.rm_rf(service_dir(name))

    provisioned_service             = ProvisionedService.new
    provisioned_service.name        = name
    provisioned_service.port        = port
    provisioned_service.plan        = plan
    provisioned_service.memory      = @max_memory
    provisioned_service.keyid       = username
    provisioned_service.secretid    = password
    provisioned_service.pid         = start_instance(provisioned_service)

    raise "Cannot save provision_service" unless provisioned_service.save

    # wait for blob to start
    sleep 0.5

    response = {
      "host" => @local_ip,
      "port" => provisioned_service.port,
      "name" => provisioned_service.name,
      "username" => username,
      "password" => password
    }
    @logger.debug("ProvisionProvision  response: #{response}")
    return response
  rescue => e
    @logger.error("Error provision instance: #{e}")
    record_service_log(provisioned_service.name)
    cleanup_service(provisioned_service)
    raise e
  end

  def unprovision(name, bindings)
    provisioned_service = ProvisionedService.get(name)
    raise ServiceError.new(ServiceError::NOT_FOUND, name) if provisioned_service.nil?
    cleanup_service(provisioned_service)
    @logger.info("Successfully fulfilled unprovision request: #{name}.")
    true
  end

  def cleanup_service(provisioned_service)
    @logger.info("Killing #{provisioned_service.name} started with pid #{provisioned_service.pid}")
    provisioned_service.kill(:SIGKILL) if provisioned_service.running?
    dir = service_dir(provisioned_service.name)
    log_dir = log_dir(provisioned_service.name)
    EM.defer do
      FileUtils.rm_rf(dir)
      FileUtils.rm_rf(log_dir)
    end
    @available_memory += provisioned_service.memory
    @free_ports << provisioned_service.port
    raise "Could not cleanup service: #{provisioned_service.errors.pretty_inspect}" unless provisioned_service.destroy
    true
  end

  # provid the key/serect to blob gw
  def bind(name, bind_opts, credential = nil)
    @logger.debug("Bind request: name=#{name}, bind_opts=#{bind_opts}")
    bind_opts ||= BIND_OPT
    provisioned_service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if provisioned_service.nil?
    username = credential && credential['username'] ? credential['username'] : UUIDTools::UUID.random_create.to_s
    password = credential && credential['password'] ? credential['password'] : UUIDTools::UUID.random_create.to_s

    blobgw_add_user({
      :port      => provisioned_service.port,
      :admin     => provisioned_service.keyid,
      :adminpass => provisioned_service.secretid,
      :username  => username,
      :password  => password,
      :bindopt   => bind_opts
    })
    response = {
      "host" => @local_ip,
      "port"     => provisioned_service.port,
      "username" => username,
      "password" => password,
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
    blobgw_remove_user({
      :port => credential['port'],
      :admin => provisioned_service.keyid,
      :adminpass => provisioned_service.secretid,
      :username => credential['username'],
      :password => credential['password']
    })
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
      # TODO: get stat from blob services
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
    # ping blob gw
    req = Net::HTTP::Get.new("/")
    res = Net::HTTP.start(@local_ip, instance.port) {|http|
      http.request(req)
    }
    res && res.code == "401" ? "ok" : "fail"
  rescue => e
    @logger.warn("Getting healthz for #{instance.inspect} failed with error #{e}")
    "fail"
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
      logdir = log_dir(provisioned_service.name);
      blob_dir = blob_dir(dir)
      log_file = log_file_blob(provisioned_service.name)
      account_file = File.join(dir,"account.json")
      keyid = provisioned_service.keyid
      secretid = provisioned_service.secretid
      config = @config_template.result(binding)
      config_path = File.join(dir, "config.json")
      FileUtils.mkdir_p(dir) rescue @logger.warn("creation failed")
      FileUtils.mkdir_p(blob_dir) rescue @logger.warn("creation failed")
      FileUtils.rm_rf(logdir) rescue @logger.warn("no such folder")
      FileUtils.mkdir_p(logdir) rescue @logger.warn("creation failed")
      FileUtils.rm_f(config_path) rescue @logger.warn("deletion falsed")
      File.open(config_path, "w") {|f| f.write(config.gsub("=>",":"))} #changing to js compatible format
      cmd = "#{@nodejs_path} #{@blobd_path}/server.js -f #{config_path}"
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

  def blobgw_add_user(options)
    @logger.debug("add user #{options[:username]} in port: #{options[:port]}")
=begin
    # encryption to be added back later
    alg = 'AES-256-CBC'
    iv = OpenSSL::Cipher::Cipher.new(alg).random_iv
    # Now we do the actual setup of the cipher
    aes = OpenSSL::Cipher::Cipher.new(alg)
    aes.encrypt
    digest = Digest::SHA256.new
    digest.update(options[:adminpass])
    key = digest.digest
    aes.key = key
    aes.iv = iv
    # Now we go ahead and encrypt our plain text.
    cipher = aes.update("#{options[:admin]}\n")
    cipher << aes.update("#{options[:username]}\n")
    cipher << aes.update("#{options[:password]}")
    cipher << aes.final
    res = Net::HTTP.start(@local_ip, options[:port]) {|http|
      http.send_request('PUT','/add_user',Base64.strict_encode64(cipher), {"x-vblob-encoding"=>"base64","x-vblob-encrypted"=>"true","x-vblob-iv" => Base64.strict_encode64(iv),"content-type"=>"text/plain"})
    }
=end
    creds = '{"'+options[:username]+'":"'+options[:password]+'"}'
    res = Net::HTTP.start(@local_ip, options[:port]) {|http|
      http.send_request('PUT','/~bind',creds)
    }
    raise "Add blobgw user failed" if (res.nil? || res.code != "200") 
    @logger.debug("user #{options[:username]} added")
  end

  def blobgw_remove_user(options)
    @logger.debug("remove user #{options[:username]} in port: #{options[:port]}")
    creds = '{"'+options[:username]+'":"'+options[:password]+'"}'
    res = Net::HTTP.start(@local_ip, options[:port]) {|http|
      http.send_request('PUT','/~unbind',creds)
    }
    raise "Delete blobgw user failed" if (res.nil? || res.code != "200")
    @logger.debug("user #{options[:username]} removed")
  end

  def service_dir(service_id)
    File.join(@base_dir, service_id)
  end

  def log_dir(instance_id)
    File.join(@blobd_log_dir,instance_id)
  end

  def log_file_blob(instance_id)
    File.join(log_dir(instance_id), 'blob.log')
  end

  def service_exist?(provisioned_service)
    Dir.exists?(service_dir(provisioned_service.name))
  end

  def blob_dir(base_dir)
    File.join(base_dir,'blob_data')
  end

  def record_service_log(service_id)
    @logger.warn(" *** BEGIN blob log - instance: #{service_id}")
    @logger.warn("")
    file = File.new(log_file_blob(service_id), 'r')
    while (line = file.gets)
      @logger.warn(line.chomp!)
    end
  rescue => e
    @logger.warn(e)
  ensure
    @logger.warn(" *** END blob log - instance: #{service_id}")
    @logger.warn("")
  end
end
