# Copyright (c) 2009-2011 VMware, Inc.
require "set"
require "uuidtools"

module VCAP
  module Services
    module Rabbit
      class Node < VCAP::Services::Base::Node
      end
    end
  end
end

require "rabbit_service/common"
require "rabbit_service/rabbit_error"
require "rabbit_service/util"

VALID_CREDENTIAL_CHARACTERS = ("A".."Z").to_a + ("a".."z").to_a + ("0".."9").to_a

class VCAP::Services::Rabbit::Node

  include VCAP::Services::Rabbit::Common
  include VCAP::Services::Rabbit::Util
  include VCAP::Services::Rabbit

  class ProvisionedService
    include DataMapper::Resource
    property :name,            String,      :key => true
    property :vhost,           String,      :required => true
    property :port,            Integer,     :unique => true
    property :admin_port,      Integer,     :unique => true
    property :admin_username,  String,      :required => true
    property :admin_password,  String,      :required => true
    property :plan,            Enum[:free], :required => true
    property :plan_option,     String,      :required => false
    property :pid,             Integer
    property :memory,          Integer,     :required => true

    def listening?
      begin
        TCPSocket.open("localhost", port).close
        return true
      rescue => e
        return false
      end
    end

    def running?
      VCAP.process_running? pid
    end

    def kill(sig=:SIGTERM)
      @wait_thread = Process.detach(pid)
      Process.kill(sig, pid) if running?
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

    @available_memory = options[:available_memory]
    @available_memory_mutex = Mutex.new
    @free_ports = Set.new
    @free_admin_ports = Set.new
    @free_ports_mutex = Mutex.new
    options[:port_range].each {|port| @free_ports << port}
    options[:admin_port_range].each {|port| @free_admin_ports << port}
    @port_gap = options[:admin_port_range].first - options[:port_range].first
    @max_memory = options[:max_memory]
    @local_db = options[:local_db]
    @binding_options = nil
    @base_dir = options[:base_dir]
    FileUtils.mkdir_p(@base_dir) if @base_dir
    @rabbitmq_server = @options[:rabbitmq_server]
    @rabbitmq_log_dir = @options[:rabbitmq_log_dir]
    @max_clients = @options[:max_clients] || 500
    # Timeout for rabbitmq client operations, node cannot be blocked on any rabbitmq instances.
    # Default value is 2 seconds.
    @rabbit_timeout = @options[:rabbit_timeout] || 2
    @default_permissions = '{"configure":".*","write":".*","read":".*"}'
  end

  def pre_send_announcement
    super
    start_db
    start_provisioned_instances
    ProvisionedService.all.each do |instance|
      @available_memory -= (instance.memory || @max_memory)
    end
  end

  def shutdown
    super
    ProvisionedService.all.each { |instance|
      @logger.debug("Try to terminate RabbitMQ server pid:#{instance.pid}")
      instance.kill
      instance.wait_killed ?
        @logger.debug("RabbitMQ server pid:#{instance.pid} terminated") :
        @logger.error("Timeout to terminate RabbitMQ server pid:#{instance.pid}")
    }
    true
  end

  def announcement
    @available_memory_mutex.synchronize do
      a = {
        :available_memory => @available_memory
      }
    end
  end

  def provision(plan, credentials = nil)
    instance = ProvisionedService.new
    instance.plan = plan
    instance.plan_option = ""
    if credentials
      instance.name = credentials["name"]
      instance.vhost = credentials["vhost"]
      instance.admin_username = credentials["user"]
      instance.admin_password = credentials["pass"]
      @free_ports_mutex.synchronize do
        if @free_ports.include?(credentials["port"])
          @free_ports.delete(credentials["port"])
          @free_admin_ports.delete(credentials["port"] + @port_gap)
          instance.port = credentials["port"]
        else
          port = @free_ports.first
          @free_ports.delete(port)
          @free_admin_ports.delete(port + @port_gap)
          instance.port = port
          instance.admin_port = port + @port_gap
        end
      end
    else
      instance.name = UUIDTools::UUID.random_create.to_s
      instance.vhost = "v" + UUIDTools::UUID.random_create.to_s.gsub(/-/, "")
      instance.admin_username = "au" + generate_credential
      instance.admin_password = "ap" + generate_credential
      port = @free_ports.first
      @free_ports.delete(port)
      @free_admin_ports.delete(port + @port_gap)
      instance.port = port
      instance.admin_port = port + @port_gap
    end
    begin
      instance.memory = memory_for_instance(instance)
      @available_memory_mutex.synchronize do
        @available_memory -= instance.memory
      end
    rescue => e
      raise e
    end
    begin
      start_instance(instance)
      instance.pid = instance_pid(instance.name)
      save_instance(instance)
    rescue => e1
      begin
        cleanup_instance(instance)
      rescue => e2
        # Ignore the rollback exception
      end
      raise e1
    end
    create_resource("guest", "guest", instance.admin_port)
    add_vhost(instance.vhost)
    add_user(instance.admin_username, instance.admin_password)
    set_permissions(instance.vhost, instance.admin_username, @default_permissions)
    create_resource(instance.admin_username, instance.admin_password, instance.admin_port)
    delete_user("guest")

    gen_credentials(instance)
  end

  def unprovision(instance_id, credentials_list = [])
    instance = get_instance(instance_id)
    cleanup_instance(instance)
    {}
  end

  def bind(instance_id, binding_options = :all, binding_credentials = nil)
    instance = get_instance(instance_id)
    user = nil
    pass = nil
    if binding_credentials
      user = binding_credentials["user"]
      pass = binding_credentials["pass"]
    else
      user = "u" + generate_credential
      pass = "p" + generate_credential
    end
    add_user(user, pass)
    set_permissions(instance.vhost, user, get_permissions_by_options(binding_options))

    gen_credentials(instance, user, pass)
  rescue => e
    # Rollback
    begin
      delete_user(user)
    rescue => e1
      # Ignore the exception here
    end
    raise e
  end

  def unbind(credentials)
    instance = get_instance(credentials["name"])
    create_resource(instance.admin_username, instance.admin_password, instance.admin_port)
    delete_user(credentials["user"])
    {}
  end

  def varz_details
    varz = {}
    varz[:provisioned_instances] = []
    varz[:provisioned_instances_num] = 0
    varz[:max_instances_num] = @options[:available_memory] / @max_memory
    ProvisionedService.all.each do |instance|
      varz[:provisioned_instances] << get_varz(instance)
      varz[:provisioned_instances_num] += 1
    end
    varz
  rescue => e
    @logger.warn(e)
    {}
  end

  def healthz_details
    healthz = {}
    healthz[:self] = "ok"
    begin
      ProvisionedService.all.each do |instance|
        healthz[instance.name.to_sym] = get_healthz(instance)
      end
    rescue => e
      @logger.warn("Error get healthz details: #{e}")
      healthz = {:self => "fail"}
    end
    healthz
  end

  def disable_instance(service_credentials, binding_credentials_list = [])
    @logger.info("disable_instance request: service_credentials=#{service_credentials}, binding_credentials=#{binding_credentials_list}")
    # Delete all binding users
    instance = get_instance(service_credentials["name"])
    create_resource(instance.admin_username, instance.admin_password, instance.admin_port)
    binding_credentials_list.each do |credentials|
      delete_user(credentials["user"])
    end
    true
  rescue => e
    @logger.warn(e)
    nil
  end

  def enable_instance(service_credentials, binding_credentials_map={})
    instance = get_instance(service_credentials["name"])
    create_resource(instance.admin_username, instance.admin_password, instance.admin_port)
    get_permissions(service_credentials["vhost"], service_credentials["user"])
    service_credentials["hostname"] = @local_ip
    service_credentials["host"] = @local_ip
    binding_credentials_map.each do |key, value|
      bind(service_credentials["name"], value["binding_options"], value["credentials"])
      binding_credentials_map[key]["credentials"]["hostname"] = @local_ip
      binding_credentials_map[key]["credentials"]["host"] = @local_ip
    end
    [service_credentials, binding_credentials_map]
  rescue => e
    @logger.warn(e)
    nil
  end

  # Rabbitmq has no data to dump for migration
  def dump_instance(service_credentials, binding_credentials_list, dump_dir)
    true
  end

  def import_instance(service_credentials, binding_credentials_map, dump_dir, plan)
    provision(plan, service_credentials)
  end

  def all_instances_list
    ProvisionedService.all.map{|s| s.name}
  end

  def all_bindings_list
    res = []
    ProvisionedService.all.each do |instance|
      create_resource(instance.admin_username, instance.admin_password, instance.admin_port)
      get_vhost_permissions(instance.vhost).each do |entry|
        credentials = {
          "name" => instance.name,
          "hostname" => @local_ip,
          "host" => @local_ip,
          "port" => instance.port,
          "vhost" => instance.vhost,
          "username" => entry["user"],
          "user" => entry["user"],
        }
        res << credentials if credentials["username"] != instance.admin_username
      end
    end
    res
  end

  def start_db
    DataMapper.setup(:default, @local_db)
    DataMapper::auto_upgrade!
  end

  def start_provisioned_instances
    ProvisionedService.all.each do |instance|
      @free_ports_mutex.synchronize do
        @free_ports.delete(instance.port)
        @free_admin_ports.delete(instance.admin_port)
      end
      if instance.listening?
        @logger.warn("Service #{instance.name} already running on port #{instance.port}")
        @available_memory_mutex.synchronize do
          @available_memory -= (instance.memory || @max_memory)
        end
        next
      end
      begin
        start_instance(instance)
        instance.pid = instance_pid(instance.name)
        save_instance(instance)
      rescue => e
        @logger.warn("Error starting instance #{instance.name}: #{e}")
        begin
          cleanup_instance(instance)
        rescue => e2
          # Ignore the rollback exception
        end
      end
    end
  end


  def save_instance(instance)
    raise RabbitError.new(RabbitError::RABBIT_SAVE_INSTANCE_FAILED, instance.inspect) unless instance.save
  end

  def destroy_instance(instance)
    raise RabbitError.new(RabbitError::RABBIT_DESTORY_INSTANCE_FAILED, instance.inspect) unless instance.destroy
  end

  def get_instance(instance_id)
    instance = ProvisionedService.get(instance_id)
    raise RabbitError.new(RabbitError::RABBIT_FIND_INSTANCE_FAILED, instance_id) if instance.nil?
    instance
  end

  def memory_for_instance(instance)
    # The memory will be decided by provision plan, now default to max_memory
    @max_memory
  end

  def start_instance(instance)
    @logger.debug("Starting: #{instance.inspect} on port #{instance.port}")

    pid = fork
    if pid
      @logger.debug("Service #{instance.name} started with pid #{pid}")
      # In parent, detch the child.
      Process.detach(pid)
      pid
    else
      $0 = "Starting RabbitMQ instance: #{instance.name}"
      close_fds

      dir = instance_dir(instance.name)
      config_dir = File.join(dir, "config")
      log_dir = File.join(@rabbitmq_log_dir, instance.name)
      FileUtils.mkdir_p(config_dir)
      FileUtils.mkdir_p(log_dir)
      # Writes the RabbitMQ server erlang configuration file
      File.open(File.join(config_dir, "rabbitmq.config"), "w") do |f|
        f.write <<EOF
  [
    {rabbit, [{vm_memory_high_watermark, 0.2}]},
    {rabbitmq_mochiweb, [{listeners, [{mgmt, [{port, #{instance.admin_port}}]}]}]}
  ].
EOF
      end
      File.open(File.join(config_dir, "enabled_plugins"), "w") do |f|
      f.write <<EOF
  [rabbitmq_management].
EOF
      end
      # Set up the environment
      {
        "RABBITMQ_NODENAME" => "#{instance.name}@localhost",
        "RABBITMQ_NODE_PORT" => instance.port.to_s,
        "RABBITMQ_BASE" => dir,
        "RABBITMQ_LOG_BASE" => log_dir,
        "RABBITMQ_MNESIA_DIR" => File.join(dir, "mnesia"),
        "RABBITMQ_PLUGINS_EXPAND_DIR" => File.join(dir, "plugins"),
        "RABBITMQ_CONFIG_FILE" => File.join(config_dir, "rabbitmq"),
        "RABBITMQ_ENABLED_PLUGINS_FILE" => File.join(config_dir, "enabled_plugins"),
        "RABBITMQ_SERVER_START_ARGS" => "-smp disable",
        "ERL_CRASH_DUMP" => "/dev/null",
        "ERL_CRASH_DUMP_SECONDS" => "1",
      }.each_pair { |k, v|
        ENV[k] = v
      }

      exec("#{@rabbitmq_server} -detached >#{log_dir}/rabbitmq_startup_log 2>&1")

    end
    # 2 seconds are enough to wait for the start of RabbitMQ server
    sleep 2
  rescue => e
    raise RabbitError.new(RabbitError::RABBIT_START_INSTANCE_FAILED, instance.inspect)
  end

  def stop_instance(instance)
    instance.kill
    EM.defer do
      FileUtils.rm_rf(instance_dir(instance.name))
    end
  end

  def cleanup_instance(instance)
    err_msg = []
    begin
      stop_instance(instance) if instance.running?
    rescue => e
      err_msg << e.message
    end
    @available_memory_mutex.synchronize do
      @available_memory += instance.memory
    end
    begin
      destroy_instance(instance)
    rescue => e
      err_msg << e.message
    end
    raise RabbitError.new(RabbitError::RABBIT_CLEANUP_INSTANCE_FAILED, err_msg.inspect) if err_msg.size > 0
  end

  def generate_credential(length = 12)
    Array.new(length) {VALID_CREDENTIAL_CHARACTERS[rand(VALID_CREDENTIAL_CHARACTERS.length)]}.join
  end

  def get_varz(instance)
    varz = {}
    varz[:name] = instance.name
    varz[:plan] = instance.plan
    varz[:vhost] = instance.vhost
    varz[:admin_username] = instance.admin_username
    varz[:usage] = {}
    @admin_url = "http://#{instance.admin_username}:#{instance.admin_password}@#{@local_ip}:#{instance.admin_port}/api"
    @rabbit_resource = RestClient::Resource.new(@admin_url, :timeout => @rabbit_timeout)
    varz[:usage][:queues_num] = list_queues(instance.vhost).size
    varz[:usage][:exchanges_num] = list_exchanges(instance.vhost).size
    varz[:usage][:bindings_num] = list_bindings(instance.vhost).size
    varz
  end

  def get_healthz(instance)
    @admin_url = "http://#{instance.admin_username}:#{instance.admin_password}@#{@local_ip}:#{instance.admin_port}/api"
    @rabbit_resource = RestClient::Resource.new(@admin_url, :timeout => @rabbit_timeout)
    get_permissions(instance.vhost, instance.admin_username) ? "ok" : "fail"
  rescue => e
    "fail"
  end

  def gen_credentials(instance, user = nil, pass = nil)
    credentials = {
      "name" => instance.name,
      "hostname" => @local_ip,
      "host" => @local_ip,
      "port"  => instance.port,
      "vhost" => instance.vhost,
    }
    if user && pass # Binding request
      credentials["username"] = user
      credentials["user"] = user
      credentials["password"] = pass
      credentials["pass"] = pass
    else # Provision request
      credentials["username"] = instance.admin_username
      credentials["user"] = instance.admin_username
      credentials["password"] = instance.admin_password
      credentials["pass"] = instance.admin_password
    end
    credentials["url"] = "amqp://#{credentials["user"]}:#{credentials["pass"]}@#{credentials["host"]}:#{credentials["port"]}/#{credentials["vhost"]}"
    credentials
  end

  def instance_dir(instance_id)
    File.join(@base_dir, instance_id)
  end

  def instance_pid(instance_id)
    pid_file = File.join(@base_dir, "#{instance_id}/mnesia.pid")
    pid = nil
    File.open(pid_file, "r") do |f|
      pid = f.gets
    end
    pid.to_i
  end

  def create_resource(username, password, port)
    @admin_url = "http://#{username}:#{password}@#{@local_ip}:#{port}/api"
    @rabbit_resource = RestClient::Resource.new(@admin_url, :timeout => @rabbit_timeout)
  end
end
