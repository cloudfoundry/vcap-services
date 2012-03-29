# Copyright (c) 2009-2011 VMware, Inc.
require "erb"
require "fileutils"
require "logger"

require "nats/client"
require "uuidtools"

require 'vcap/common'
require 'vcap/component'
require "mongodb_service/common"

module VCAP
  module Services
    module MongoDB
      class Node < VCAP::Services::Base::Node
      end
    end
  end
end

class VCAP::Services::MongoDB::Node

  include VCAP::Services::MongoDB::Common

  # FIXME only support rw currently
  BIND_OPT = 'rw'

  # Timeout for mongo client operations, node cannot be blocked on any mongo instances.
  # Default value is 2 seconds
  MONGO_TIMEOUT = 2

  # Max clients' connection number per instance
  MAX_CLIENTS = 500

  # Quota files specify the db quota a instance can use
  QUOTA_FILES = 4

  def initialize(options)
    super(options)
    ProvisionedService.init(options)
    @free_ports = options[:port_range].to_a
    @mutex = Mutex.new
  end

  def new_port(port=nil)
    @mutex.synchronize do
      return @free_ports.shift if port.nil?
      raise "port #{port} is already taken!" unless @free_ports.include?(port)
      @free_ports.delete(port)
      port
    end
  end

  def free_port(port)
    @mutex.synchronize do
      raise "port #{port} already freed!" if @free_ports.include?(port)
      @free_ports << port
    end
  end

  def del_port(port)
    @mutex.synchronize do
      @free_ports.delete(port)
    end
  end

  def port_occupied?(port)
    begin
      TCPSocket.open('localhost', port).close
      return true
    rescue => e
      return false
    end
  end

  def pre_send_announcement
    @capacity_lock.synchronize do
      ProvisionedService.all.each do |p_service|
        @capacity -= capacity_unit
        del_port(p_service.port)
        if p_service.running? then
          @logger.warn("Service #{p_service.name} already listening on port #{p_service.port}")
          next
        end

        unless p_service.service_dir?
          @logger.warn("Service #{p_service.name} in local DB, but not in file system")
          next
        end

        begin
          p_service.run
        rescue => e
          puts e
          p_service.stop
          @logger.error("Error starting service #{p_service.name}: #{e}")
        end
      end
    end
  end

  def shutdown
    super
    @logger.info("Shutting down instances..")
    ProvisionedService.all.each do |p_service|
      @logger.debug("Try to terminate mongod container:#{p_service.pid}")
      p_service.stop
    end
  end

  def announcement
    @capacity_lock.synchronize do
      { :available_capacity => @capacity,
        :capacity_unit => capacity_unit }
    end
  end

  def all_instances_list
    ProvisionedService.all.map { |p_service| p_service["name"] }
  end

  def all_bindings_list
    list = []
    ProvisionedService.all.each do |p_service|
      begin
        conn = p_service.connect
        coll = conn.db(p_service.db).collection('system.users')
        coll.find().each do |binding|
          next if binding['user'] == p_service.admin
          list << {
            'name'     => p_service.name,
            'port'     => p_service.port,
            'db'       => p_service.db,
            'username' => binding['user']
          }
        end
        p_service.disconnect(conn)
      rescue => e
        @logger.warn("Failed fetch user list: #{e.message}")
      end
    end
    list
  end

  def provision(plan, credential = {})
    @logger.info("Provision request: plan=#{plan}")
    credential['plan'] = plan
    credential['port'] = new_port(credential['port'])
    p_service = ProvisionedService.create(credential)
    p_service.run

    username = credential['username'] ? credential['username'] : UUIDTools::UUID.random_create.to_s
    password = credential['password'] ? credential['username'] : UUIDTools::UUID.random_create.to_s
    p_service.add_admin(p_service.admin, p_service.adminpass)
    p_service.add_user(p_service.admin, p_service.adminpass)
    p_service.add_user(username, password)

    host = get_host
    response = {
      "hostname" => host,
      "host"     => host,
      "port"     => p_service.port,
      "name"     => p_service.name,
      "db"       => p_service.db,
      "username" => username,
      "password" => password
    }
    @logger.debug("Provision response: #{response}")
    return response
  rescue => e
    puts e
    @logger.error("Error provision instance: #{e}")
    cleanup_service(p_service) unless p_service.nil?
    raise e
  end

  def unprovision(name, bindings)
    p_service = ProvisionedService.get(name)
    port = p_service.port
    raise ServiceError.new(ServiceError::NOT_FOUND, name) if p_service.nil?
    raise "Could not cleanup service #{p_service.errros.inspect}" unless p_service.delete
    free_port(port);
    @logger.info("Successfully fulfilled unprovision request: #{name}.")
    true
  end

  def bind(name, bind_opts, credential = nil)
    @logger.info("Bind request: name=#{name}, bind_opts=#{bind_opts}")
    bind_opts ||= BIND_OPT

    p_service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if p_service.nil?

    username = credential && credential['username'] ? credential['username'] : UUIDTools::UUID.random_create.to_s
    password = credential && credential['password'] ? credential['password'] : UUIDTools::UUID.random_create.to_s

    p_service.add_user(username, password)

    host = get_host
    response = {
      "hostname" => host,
      "host"     => host,
      "port"     => p_service.port,
      "username" => username,
      "password" => password,
      "name"     => p_service.name,
      "db"       => p_service.db
    }

    @logger.debug("Bind response: #{response}")
    response
  end

  def unbind(credential)
    @logger.info("Unbind request: credential=#{credential}")
    p_service = ProvisionedService.get(credential['name'])
    raise ServiceError.new(ServiceError::NOT_FOUND, name) if p_service.nil?

    if p_service.port != credential['port'] or
       p_service.db != credential['db']
      raise ServiceError.new(ServiceError::HTTP_BAD_REQUEST)
    end

    # FIXME  Current implementation: Delete self
    #        Here I presume the user to be deleted is RW user
    p_service.remove_user(credential['username'])

    @logger.debug("Successfully unbind #{credential}")
    true
  end

  def restore(instance_id, backup_file)
    @logger.info("Restore request: instance_id=#{instance_id}, backup_file=#{backup_file}")

    p_service = ProvisionedService.get(instance_id)
    raise ServiceError.new(ServiceError::NOT_FOUND, instance_id) if p_service.nil?

    p_service.d_import(backup_file)
  end

  def disable_instance(service_credential, binding_credentials)
    @logger.info("disable_instance request: service_credential=#{service_credential}, binding_credentials=#{binding_credentials}")
    p_service = ProvisionedService.get(service_credential['name'])
    raise ServiceError.new(ServiceError::NOT_FOUND, service_credential['name']) if p_service.nil?
    p_service.stop if p_service.running?
    true
  rescue => e
    @logger.warn(e)
    nil
  end

  def dump_instance(service_credential, binding_credentials, dump_dir)
    @logger.info("dump_instance request: service_credential=#{service_credential}, binding_credentials=#{binding_credentials}, dump_dir=#{dump_dir}")

    p_service = ProvisionedService.get(service_credential['name'])
    raise "Cannot find service #{service_credential['name']}" if p_service.nil?
    FileUtils.mkdir_p(dump_dir)
    p_service.dump(dump_dir)
    true
  rescue => e
    @logger.warn(e)
    nil
  end

  def import_instance(service_credential, binding_credentials, dump_dir, plan)
    @logger.info("import_instance request: service_credential=#{service_credential}, binding_credentials=#{binding_credentials}, dump_dir=#{dump_dir}, plan=#{plan}")

    # Load Provisioned Service from dumped file
    port = new_port
    p_service = ProvisionedService.import(port, dump_dir)
    true
  rescue => e
    @logger.warn(e)
    puts e
    nil
  end

  def enable_instance(service_credential, binding_credentials)
    @logger.info("enable_instance request: service_credential=#{service_credential}, binding_credentials=#{binding_credentials}")
    # Load provisioned_service from dumped file
    p_service = ProvisionedService.get(service_credential['name'])
    raise "Cannot find service #{service_credential['name']}" if p_service.nil?

    p_service.run
    host = get_host

    # Update credentials for the new credential
    service_credential['port']     = p_service.port
    service_credential['host']     = host
    service_credential['hostname'] = host

    binding_credentials.each_value do |value|
      v = value["credentials"]
      v['port']     = p_service.port
      v['host']     = host
      v['hostname'] = host
    end

    [service_credential, binding_credentials]
  rescue => e
    @logger.warn(e)
    p_service.delete if p_service
    nil
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

    # Get mongodb db.stats and db.serverStatus
    stats = ProvisionedService.all.map do |p_service|
      stat = {}
      stat['overall'] = p_service.overall_stats
      stat['db']      = p_service.db_stats
      stat['name']    = p_service.name
      stat
    end

    # Get service instance status
    provisioned_instances = {}
    begin
      ProvisionedService.all.each do |p_service|
        provisioned_instances[p_service.name.to_sym] = p_service.get_healthz
      end
    rescue => e
      @logger.error("Error get instance list: #{e}")
    end

    {
      :running_services     => stats,
      :disk                 => du_hash,
      :max_capacity         => @max_capacity,
      :available_capacity   => @capacity,
      :instances            => provisioned_instances
    }
  end

  def healthz_details
    healthz = {}
    healthz[:self] = "ok"
    ProvisionedService.all.each do |p_service|
      healthz[p_service.name.to_sym] = p_service.get_healthz
    end
    healthz
  rescue => e
    @logger.warn("Error get healthz details: #{e}")
    {:self => "fail"}
  end
end
