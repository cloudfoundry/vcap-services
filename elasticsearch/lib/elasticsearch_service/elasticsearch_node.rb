# Copyright (c) 2009-2011 VMware, Inc.
require "erb"
require "fileutils"
require "logger"
require "pp"
require "set"

require "nats/client"
require "uuidtools"

require 'vcap/common'
require 'vcap/component'
require "elasticsearch_service/common"
require 'rest-client'
require 'net/http'

module VCAP
  module Services
    module ElasticSearch
      class Node < VCAP::Services::Base::Node
      end
    end
  end
end

class VCAP::Services::ElasticSearch::Node

  include VCAP::Services::ElasticSearch::Common

  class ProvisionedService
    include DataMapper::Resource
    property :name,       String,       :key => true
    property :port,       Integer,      :unique => true
    property :password,   String,       :required => true
    property :plan,       Enum[:free],  :required => true
    property :pid,        Integer
    property :memory,     Integer
    property :username,   String,       :required => true

    def listening?
      begin
        TCPSocket.open('localhost', port).close
        return true
      rescue => e
        return false
      end
    end

    def running?
      VCAP.process_running? pid
    end

    def kill(sig=9)
      Process.kill(sig, pid) if running?
    end
  end

  def initialize(options)
    super(options)
    @base_dir = options[:base_dir]
    FileUtils.mkdir_p(@base_dir)
    @elasticsearch_path = options[:elasticsearch_path]
    @max_memory = options[:max_memory]
    @config_template = ERB.new(File.read(options[:config_template]))
    @init_script_template = ERB.new(File.read(options[:init_script_template]))

    DataMapper.setup(:default, options[:local_db])
    DataMapper::auto_upgrade!

    @free_ports = Set.new
    options[:port_range].each {|port| @free_ports << port}
    @mutex = Mutex.new
  end

  def pre_send_announcement
    @capacity_lock.synchronize do
      ProvisionedService.all.each do |provisioned_service|
        @capacity -= capacity_unit
        delete_port(provisioned_service.port)
        if provisioned_service.listening?
          @logger.info("Service #{provisioned_service.name} already listening on port #{provisioned_service.port}")
          next
        end
        begin
          pid = start_instance(provisioned_service)
          provisioned_service.pid = pid
          unless provisioned_service.save
            provisioned_service.kill
            raise "Couldn't save pid (#{pid})"
          end
        rescue => e
          @logger.error("Error starting service #{provisioned_service.name}: #{e}")
        end
      end
    end
  end

  def shutdown
    super
    @logger.info("Shutting down instances..")
    ProvisionedService.all.each do |service|
      @logger.info("Shutting down #{service}")
      stop_service(service)
    end
  end

  def announcement
    @capacity_lock.synchronize do
      { :available_capacity => @capacity,
        :capacity_unit => capacity_unit }
    end
  end

  def all_instances_list
    ProvisionedService.all.map{ |ps| ps["name"] }
  end

  def all_bindings_list
    list = []
    ProvisionedService.all.each do |ps|
      next unless ps.listening?
      begin
        RestClient.get "http://#{ps.username}:#{ps.password}@#{@local_ip}:#{ps.port}/#{ps.name}"
      rescue RestClient::Unauthorized
        next
      rescue
      end
      credential = {
        'name' => ps.name,
        'port' => ps.port,
        'username' => ps.username
      }
      list << credential
    end
    list
  end

  def provision(plan, credentials = nil)
    provisioned_service = ProvisionedService.new
    if credentials
      provisioned_service.name = credentials["name"]
      provisioned_service.username = credentials["username"]
      provisioned_service.password = credentials["password"]
    else
      provisioned_service.name = "elasticsearch-#{UUIDTools::UUID.random_create.to_s}"
      provisioned_service.username = UUIDTools::UUID.random_create.to_s
      provisioned_service.password = UUIDTools::UUID.random_create.to_s
    end

    provisioned_service.port = fetch_port
    provisioned_service.plan = plan
    provisioned_service.memory = @max_memory
    provisioned_service.pid = start_instance(provisioned_service)

    unless provisioned_service.pid && provisioned_service.save
      cleanup_service(provisioned_service)
      raise "Could not save entry: #{provisioned_service.errors.pretty_inspect}"
    end

    response = get_credentials(provisioned_service)
    @logger.debug("response: #{response}")
    return response
  rescue => e
    @logger.warn(e)
  end

  def unprovision(name, credentials = nil)
    provisioned_service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if provisioned_service.nil?

    cleanup_service(provisioned_service)
    @logger.debug("Successfully fulfilled unprovision request: #{name}.")
  end

  # FIXME Elasticsearch has no user level security, just return provisioned credentials.
  # Elasticsearch has not built-in user authentication system.
  # So "http-basic(https://github.com/Asquera/elasticsearch-http-basic)" plugin
  # is added for authentication. But It has not support multi-user authentication.
  # It supports only 1 user per 1 instance. Provisioned credentials does not changed
  # regardless of any bind requests.
  def bind(name, bind_opts = 'rw', credentials = nil)
    @logger.debug("Bind request: name=#{name}, bind_opts=#{bind_opts}")

    provisioned_service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if provisioned_service.nil?

    response = get_credentials(provisioned_service)
    @logger.debug("response: #{response}")
    response
  end

  # FIXME Elasticsearch has no user level security, just return.
  def unbind(credentials)
    @logger.debug("Unbind request: credentials=#{credentials}")

    name = credentials['name']
    provisioned_service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if provisioned_service.nil?

    @logger.debug("Successfully unbound #{credentials}")
    true
  end

  def start_instance(provisioned_service)
    @logger.debug("Starting: #{provisioned_service.pretty_inspect}")

    memory = @max_memory
    name = provisioned_service.name
    dir = File.join(@base_dir, name)
    FileUtils.mkdir_p(dir)

    setup_server(dir, provisioned_service) unless File.directory? "#{dir}/data"

    init_script = File.join(dir, "bin", "elasticsearch_ctl.sh")
    @logger.info("Calling #{init_script} start")

    out = `cd #{dir} && #{init_script} start`
    status = $?
    @logger.send(status.success? ? :debug : :error, "Init finished, status = #{status}, out = #{out}")

    pidfile = File.join(dir, "elasticsearch.pid")

    pid = `[ -f #{pidfile} ] && cat #{pidfile}`
    status = $?
    @logger.send(status.success? ? :debug : :error, "Service #{name} running with pid #{pid}")

    return pid.to_i
  end

  def setup_server(dir, provisioned_service)
    @logger.info("Installing elasticsearch to #{dir}")

    auth_plugin_dir = "#{dir}/plugins/http-basic"
    `cd #{dir} && tar xfz #{@elasticsearch_path}/elasticsearch-server.tgz --strip=1`
    `mkdir -p #{auth_plugin_dir} && cp #{@elasticsearch_path}/elasticsearch-http-basic.jar #{auth_plugin_dir}`

    home_dir = dir
    name = provisioned_service.name
    port = provisioned_service.port
    password = provisioned_service.password
    username = provisioned_service.username
    memory= @max_memory

    File.open(File.join(dir, "config", "elasticsearch.yml"), "w") do |f|
      f.write(@config_template.result(binding))
    end

    File.open(File.join(dir, "bin", "elasticsearch_ctl.sh"), "w") do |f|
      f.write(@init_script_template.result(binding))
      f.chmod(0755)
    end
  end

  def get_credentials(provisioned_service)
    raise "Could not access provisioned service" unless provisioned_service
    credentials = {
      "hostname" => @local_ip,
      "host"     => @local_ip,
      "port"     => provisioned_service.port,
      "username" => provisioned_service.username,
      "password" => provisioned_service.password,
      "name"     => provisioned_service.name,
    }
    credentials["url"] = "http://#{credentials['username']}:#{credentials['password']}@#{credentials['host']}:#{credentials['port']}"
    credentials
  end

  def cleanup_service(provisioned_service)
    @logger.debug("Killing #{provisioned_service.name} started with pid #{provisioned_service.pid}")

    stop_service(provisioned_service)

    raise "Could not cleanup service: #{provisioned_service.errors.pretty_inspect}" unless provisioned_service.new? || provisioned_service.destroy

    Process.kill(9, provisioned_service.pid) if provisioned_service.running?
    dir = File.join(@base_dir, provisioned_service.name)

    EM.defer { FileUtils.rm_rf(dir) }

    return_port(provisioned_service.port)

    true
  rescue => e
    @logger.warn(e)
  end

  def stop_service(service)
    begin
      @logger.info("Stopping #{service.name} PORT #{service.port} PID #{service.pid}")
      init_script = File.join(@base_dir, service.name, "bin", "elasticsearch_ctl.sh")
      @logger.info("Calling #{init_script} stop")
      out = `#{init_script} stop`
      stopped = $?
      @logger.debug("Stop finished, status = #{stopped}, out = #{out}")
    rescue => e
      @logger.error("Error stopping service #{service.name} PORT #{service.port} PID #{service.pid}: #{e}")
    end
    service.kill(:SIGTERM) if service.running?
  end

  def fetch_port(port=nil)
    @mutex.synchronize do
      port ||= @free_ports.first
      raise "port #{port} is already taken!" unless @free_ports.include?(port)
      @free_ports.delete(port)
      port
    end
  end

  def return_port(port)
    @mutex.synchronize do
      @free_ports << port
    end
  end

  def delete_port(port)
    @mutex.synchronize do
      @free_ports.delete(port)
    end
  end
end
