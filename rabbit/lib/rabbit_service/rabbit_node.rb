# Copyright (c) 2009-2011 VMware, Inc.
require "set"
require "open3"
require "uuidtools"
require "vcap/common"
require "vcap/component"
require "warden/client"
require "posix/spawn"
require "rabbit_service/common"
require "rabbit_service/rabbit_error"
require "rabbit_service/util"

module VCAP
  module Services
    module Rabbit
      class Node < VCAP::Services::Base::Node
        class ProvisionedService
        end
      end
    end
  end
end

VALID_CREDENTIAL_CHARACTERS = ("A".."Z").to_a + ("a".."z").to_a + ("0".."9").to_a
def generate_credential(length = 12)
  Array.new(length) {VALID_CREDENTIAL_CHARACTERS[rand(VALID_CREDENTIAL_CHARACTERS.length)]}.join
end

class VCAP::Services::Rabbit::Node

  include VCAP::Services::Rabbit::Common
  include VCAP::Services::Rabbit::Util
  include VCAP::Services::Rabbit
  include VCAP::Services::Base::Utils

  def initialize(options)
    super(options)
    @free_ports = Set.new
    @free_ports_lock = Mutex.new
    options[:port_range].each {|port| @free_ports << port}
    options[:max_clients] ||= 500
    options[:max_memory_factor] ||= 0.5
    options[:max_capacity] = @max_capacity
    # Configuration used in warden
    @rabbitmq_port = options[:instance_port] = 10001
    @rabbitmq_admin_port = options[:instance_admin_port] = 20001
    # Timeout for redis client operations, node cannot be blocked on any redis instances.
    # Default value is 2 seconds.
    @rabbitmq_timeout = @options[:rabbitmq_timeout] || 2
    @rabbitmq_start_timeout = @options[:rabbitmq_start_timeout] || 5
    @default_permissions = '{"configure":".*","write":".*","read":".*"}'
    @initial_username = "guest"
    @initial_password = "guest"
    @hostname = get_host
    ProvisionedService.init(options)
    @options = options
  end

  def pre_send_announcement
    @capacity_lock.synchronize do
      ProvisionedService.all.each do |instance|
        @capacity -= capacity_unit
        del_port(instance.port)

        if instance.running? then
          @logger.warn("Service #{instance.name} already listening on port #{instance.port}")
          next
        end

        unless instance.base_dir?
          @logger.warn("Service #{instance.name} in local DB, but not in file system")
          next
        end

        instance.migration_check

        begin
          instance.run
          raise RabbitmqError.new(RabbitmqError::RABBITMQ_START_INSTANCE_TIMEOUT, instance.name) if wait_rabbitmq_server_start(instance, false) == false
          @logger.info("Successfully start provisioned instance #{instance.name}")
        rescue => e
          @logger.error("Error starting instance #{instance.name}: #{e}")
          instance.stop
        end
      end
    end
  end

  def shutdown
    super
    @logger.info("Shutting down instances..")
    ProvisionedService.all.each do |instance|
      @logger.debug("Try to terminate rabbitmq container: #{instance.name}")
      instance.stop if instance.running?
    end
  end

  def announcement
    @capacity_lock.synchronize do
      { :available_capacity => @capacity,
        :capacity_unit => capacity_unit }
    end
  end

  def provision(plan = nil, credentials = nil)
    port = nil
    instance = nil
    if credentials
      port = new_port(credentials["port"])
      instance = ProvisionedService.create(port, get_admin_port(port), plan, credentials)
    else
      port = new_port
      instance = ProvisionedService.create(port, get_admin_port(port), plan)
    end
    instance.run
    # Wait enough time for the RabbitMQ server starting
    raise RabbitmqError.new(RabbitmqError::RABBITMQ_START_INSTANCE_TIMEOUT, instance.name) if wait_rabbitmq_server_start(instance) == false
    # Use initial credentials to create provision user
    credentials = {"username" => @initial_username, "password" => @initial_password, "hostname" => instance.ip}
    add_vhost(credentials, instance.vhost)
    add_user(credentials, instance.admin_username, instance.admin_password)
    set_permissions(credentials, instance.vhost, instance.admin_username, @default_permissions)
    # Use provision user credentials to delete initial user for security
    credentials["username"] = instance.admin_username
    credentials["password"] = instance.admin_password
    delete_user(credentials, @initial_username)
    @logger.info("Successfully fulfilled provision request: #{instance.name}")
    gen_credentials(instance)
  rescue => e
    @logger.error("Error provision instance: #{e}")
    instance.delete if instance
    free_port(port) if port
    raise e
  end

  def unprovision(name, credentials_list = [])
    instance = ProvisionedService.get(name)
    raise ServiceError.new(ServiceError::NOT_FOUND, name) if instance.nil?
    port = instance.port
    raise "Could not cleanup instance #{name}" unless instance.delete
    free_port(port)
    @logger.info("Successfully fulfilled unprovision request: #{name}")
    true
  end

  def bind(instance_id, binding_options = :all, binding_credentials = nil)
    instance = ProvisionedService.get(instance_id)
    user = nil
    pass = nil
    if binding_credentials
      user = binding_credentials["user"]
      pass = binding_credentials["pass"]
    else
      user = "u" + generate_credential
      pass = "p" + generate_credential
    end
    credentials = gen_admin_credentials(instance)
    add_user(credentials, user, pass)
    set_permissions(credentials, instance.vhost, user, get_permissions_by_options(binding_options))

    binding_credentials = gen_credentials(instance, user, pass)
    @logger.info("Successfully fulfilled bind request: #{binding_credentials}")
    binding_credentials
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
    instance = ProvisionedService.get(credentials["name"])
    delete_user(gen_admin_credentials(instance), credentials["user"])
    @logger.info("Successfully fulfilled unbind request: #{credentials}")
    {}
  end

  def varz_details
    varz = {}
    varz[:provisioned_instances] = []
    varz[:provisioned_instances_num] = 0
    varz[:max_capacity] = @max_capacity
    varz[:available_capacity] = @capacity
    varz[:instances] = {}
    ProvisionedService.all.each do |instance|
      varz[:instances][instance.name.to_sym] = get_status(instance)
      varz[:provisioned_instances_num] += 1
      begin
        varz[:provisioned_instances] << get_varz(instance)
      rescue => e
        @logger.warn("Failed to get instance #{instance.name} varz details: #{e}")
      end
    end
    varz
  rescue => e
    @logger.warn(e)
    {}
  end

  def disable_instance(service_credentials, binding_credentials_list = [])
    @logger.info("disable_instance request: service_credentials=#{service_credentials}, binding_credentials=#{binding_credentials_list}")
    instance = ProvisionedService.get(service_credentials["name"])
    # Delete all binding users
    binding_credentials_list.each do |credentials|
      delete_user(gen_admin_credentials(instance), credentials["user"])
    end
    true
  rescue => e
    @logger.warn(e)
    nil
  end

  # Rabbitmq has no data to dump for migration
  def dump_instance(service_credentials, binding_credentials_list, dump_dir)
    true
  end

  def enable_instance(service_credentials, binding_credentials_map = {})
    @logger.info("enable_instance request: service_credentials=#{service_credentials}, binding_credentials=#{binding_credentials_map}")
    instance = ProvisionedService.get(service_credentials["name"])
    # Add all binding users
    binding_credentials_map.each do |_, value|
      bind(service_credentials["name"], value["binding_options"], value["credentials"])
    end
    true
  rescue => e
    @logger.warn(e)
    nil
  end

  def import_instance(service_credentials, binding_credentials_map, dump_dir, plan)
    provision(plan, service_credentials)
    true
  end

  def update_instance(service_credentials, binding_credentials_map={})
    instance = ProvisionedService.get(service_credentials["name"])
    service_credentials["hostname"] = @hostname
    service_credentials["host"] = @hostname
    binding_credentials_map.each do |key, value|
      bind(service_credentials["name"], value["binding_options"], value["credentials"])
      binding_credentials_map[key]["credentials"]["hostname"] = @hostname
      binding_credentials_map[key]["credentials"]["host"] = @hostname
    end
    [service_credentials, binding_credentials_map]
  rescue => e
    @logger.warn(e)
    nil
  end

  def all_instances_list
    ProvisionedService.all.map{|s| s.name}
  end

  def all_bindings_list
    res = []
    ProvisionedService.all.each do |instance|
      get_vhost_permissions(gen_admin_credentials(instance), instance.vhost).each do |entry|
        credentials = {
          "name" => instance.name,
          "hostname" => @hostname,
          "host" => @hostname,
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

  def new_port(port=nil)
    @free_ports_lock.synchronize do
      raise "No ports available." if @free_ports.empty?
      if port.nil? || !@free_ports.include?(port)
        port = @free_ports.first
        @free_ports.delete(port)
      else
        @free_ports.delete(port)
      end
    end
    port
  end

  def free_port(port)
    @free_ports_lock.synchronize do
      raise "port #{port} already freed!" if @free_ports.include?(port)
      @free_ports.add(port)
    end
  end

  def del_port(port)
    @free_ports_lock.synchronize do
      @free_ports.delete(port)
    end
  end

  def get_varz(instance)
    varz = {}
    varz[:name] = instance.name
    varz[:plan] = @plan
    varz[:vhost] = instance.vhost
    varz[:admin_username] = instance.admin_username
    varz[:usage] = {}
    credentials = gen_admin_credentials(instance)
    varz[:usage][:queues_num] = list_queues(credentials, instance.vhost).size
    varz[:usage][:exchanges_num] = list_exchanges(credentials, instance.vhost).size
    varz[:usage][:bindings_num] = list_bindings(credentials, instance.vhost).size
    varz
  end

  def get_status(instance)
    get_permissions(gen_admin_credentials(instance), instance.vhost, instance.admin_username) ? "ok" : "fail"
  rescue => e
    "fail"
  end

  def gen_credentials(instance, user = nil, pass = nil)
    credentials = {
      "name" => instance.name,
      "hostname" => @hostname,
      "host" => @hostname,
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

  def gen_admin_credentials(instance)
    credentials = {
      "hostname"  => instance.ip,
      "username" => instance.admin_username,
      "password" => instance.admin_password,
    }
  end

  def get_admin_port(port)
    port + 10000
  end

  def get_instance(name)
    instance = ProvisionedService.get(name)
    raise RabbitmqError.new(RabbitmqError::RABBITMQ_FIND_INSTANCE_FAILED, name) if instance.nil?
    instance
  end

  def wait_rabbitmq_server_start(instance, is_init=true)
    @rabbitmq_start_timeout.times do
      sleep 1
      if is_init # A new instance
        credentials = {"username" => @initial_username, "password" => @initial_password, "hostname" => instance.ip}
      else # An existed instance
        credentials = {"username" => instance.admin_username, "password" => instance.admin_password, "hostname" => instance.ip}
      end
      begin
        # Try to call management API, if success, then return
        response = create_resource(credentials)["users"].get
        JSON.parse(response)
        return true
      rescue => e
        next
      end
    end
    false
  end

end

class VCAP::Services::Rabbit::Node::ProvisionedService

  include DataMapper::Resource
  include VCAP::Services::Rabbit
  include VCAP::Services::Base::Utils
  include VCAP::Services::Base::Warden

  property :name,            String,      :key => true
  property :vhost,           String,      :required => true
  property :port,            Integer,     :unique => true
  property :admin_port,      Integer,     :unique => true
  property :admin_username,  String,      :required => true
  property :admin_password,  String,      :required => true
  # property plan is deprecated. The instances in one node have same plan.
  property :plan,            Integer,     :required => true
  property :plan_option,     String,      :required => false
  property :pid,             Integer
  property :memory,          Integer,     :required => true
  property :status,          Integer,     :default => 0
  property :container,       String
  property :ip,              String

  private_class_method :new

  class << self

    def init(options)
      @@options = options
      @base_dir = options[:base_dir]
      @log_dir = options[:rabbitmq_log_dir]
      @image_dir = options[:image_dir]
      @logger = options[:logger]
      @max_db_size = options[:max_db_size]
      @quota = options[:filesystem_quota] || false
      FileUtils.mkdir_p(options[:base_dir])
      FileUtils.mkdir_p(options[:rabbitmq_log_dir])
      FileUtils.mkdir_p(options[:image_dir])
      DataMapper.setup(:default, options[:local_db])
      DataMapper::auto_upgrade!
    end

    def create(port, admin_port, plan=nil, credentials=nil)
      raise "Parameter missing" unless port && admin_port
      # The instance could be an old instance without warden support
      instance = get(credentials["name"]) if credentials
      instance = new if instance == nil
      instance.port = port
      instance.admin_port = port
      if credentials
        instance.name = credentials["name"]
        instance.vhost = credentials["vhost"]
        instance.admin_username = credentials["username"]
        instance.admin_password = credentials["password"]
      else
        instance.name = UUIDTools::UUID.random_create.to_s
        instance.vhost = "v" + UUIDTools::UUID.random_create.to_s.gsub(/-/, "")
        instance.admin_username = "au" + generate_credential
        instance.admin_password = "ap" + generate_credential
      end
      # These properties are deprecated
      instance.memory = 1
      instance.plan = 1
      instance.plan_option = "rw"
      instance.pid = 0

      raise "Cannot save provision service" unless instance.save!

      # Generate configuration
      port = @@options[:instance_port]
      admin_port = @@options[:instance_admin_port]
      # To allow for garbage-collection, http://www.rabbitmq.com/memory.html recommends that vm_memory_high_watermark be set to 40%.
      # But since we run up to max_capacity instances on each node, we must give each instance less than 40% of the memory.
      # Analysis of the worst case (all instances are very busy and doing GC at the same time) suggests that we should set vm_memory_high_watermark = 0.4 / max_capacity.
      # But we do not expect to ever see this worst-case situation in practice, so we
      # (a) allow a numerator different from 40%, max_memory_factor defaults to 50%;
      # (b) make the number grow more slowly as of max_capacity increases.
      vm_memory_high_watermark = @@options[:max_memory_factor] / (1 + Math.log(@@options[:max_capacity]))
      # In RabbitMQ, If the file_handles_high_watermark is x, then the socket limitation is x * 0.9 - 2,
      # to let the @max_clients be a more accurate limitation, the file_handles_high_watermark will be set to
      # (@max_clients + 3) / 0.9
      file_handles_high_watermark = ((@@options[:max_clients] + 2) / 0.9).to_i
      # Writes the RabbitMQ server erlang configuration file
      config_template = ERB.new(File.read(@@options[:config_template]))
      config = config_template.result(Kernel.binding)
      config_path = File.join(instance.config_dir, "rabbitmq.config")
      begin
        Open3.capture3("umount #{instance.base_dir}") if File.exist?(instance.base_dir)
      rescue => e
      end
      FileUtils.rm_rf(instance.base_dir)
      FileUtils.rm_rf(instance.log_dir)
      FileUtils.rm_rf(instance.image_file)
      FileUtils.mkdir_p(instance.base_dir)
      instance.prepare_filesystem(max_db_size)
      FileUtils.mkdir_p(instance.config_dir)
      FileUtils.mkdir_p(instance.log_dir)
      # Writes the RabbitMQ server erlang configuration file
      File.open(config_path, "w") {|f| f.write(config)}
      # Enable management plugin
      File.open(File.join(instance.config_dir, "enabled_plugins"), "w") do |f|
        f.write <<EOF
[rabbitmq_management].
EOF
      end
      instance
    end
  end

  def service_port
    @@options[:instance_port]
  end

  def service_script
    "rabbitmq_startup.sh #{self[:name]}"
  end

  def migration_check
    super
    if container == nil
      # Regenerate the configuration, need change the port to instance_admin_port
      config_file = File.join(config_dir, "rabbitmq.config")
      content = File.read(config_file)
      content = content.gsub(/port, \d{5}/, "port, #{@@options[:instance_admin_port]}")
      File.open(config_file, "w") {|f| f.write(content)}
    end
  end

  # diretory helper
  def config_dir
    File.join(base_dir, "config")
  end

end
