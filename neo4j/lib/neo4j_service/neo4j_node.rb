# Copyright (c) 2009-2011 VMware, Inc.
require "erb"
require "fileutils"
require "logger"
require "pp"
require "set"

require "datamapper"
require "nats/client"
require "uuidtools"

require 'vcap/common'
require 'vcap/component'
require "neo4j_service/common"
require 'rest-client'

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..', 'base', 'lib')
require 'base/node'

module VCAP
  module Services
    module Neo4j
      class Node < VCAP::Services::Base::Node
      end
    end
  end
end

class VCAP::Services::Neo4j::Node

  include VCAP::Services::Neo4j::Common

  # FIXME only support rw currently
  BIND_OPT = 'rw'

  class ProvisionedService
    include DataMapper::Resource
    property :name,       String,   :key => true
    property :port,       Integer,  :unique => true
    property :password,   String,   :required => true
    property :plan,       Enum[:free], :required => true
    property :pid,        Integer
    property :memory,     Integer
    property :username,      String,   :required => true

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
    @neo4j_path = options[:neo4j_path]

    @available_memory = options[:available_memory]
    @max_memory = options[:max_memory]

    @config_template = ERB.new(File.read(options[:config_template]))
    @db_template = ERB.new(File.read(options[:neo4j_template]))
    @log_template = ERB.new(File.read(options[:log4j_template]))

    DataMapper.setup(:default, options[:local_db])
    DataMapper::auto_upgrade!

    @free_ports = Set.new
    options[:port_range].each {|port| @free_ports << port}

    ProvisionedService.all.each do |provisioned_service|
      @free_ports.delete(provisioned_service.port)
      if provisioned_service.listening?
        @logger.info("Service #{provisioned_service.name} already listening on port #{provisioned_service.port}")
        @available_memory -= (provisioned_service.memory || @max_memory)
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
        @logger.warn("Error starting service #{provisioned_service.name}: #{e}")
      end
    end

  end

  def shutdown
    super
    @logger.info("Shutting down instances..")
    ProvisionedService.all.each do |service|
      @logger.info("Shutting down #{service}")
    end
    ProvisionedService.all.each do |service|
      stop_service(service)
    end
  end

  def stop_service(service)
    begin
      @logger.info("Stopping #{service.name} PORT #{service.port} PID #{service.pid}")
      init_script = File.join(@base_dir,service.name,"bin","neo4j")
      @logger.info("Calling #{init_script} stop")
      
      fork do
        handle = IO.popen("#{init_script} stop", { close_fds => 1 })
        @logger.debug("finished stop #{ handle }")
      end
      Process.wait      
      
    rescue => e
      @logger.error("Error stopping service #{service.name} PORT #{service.port} PID #{service.pid}: #{e}")
    end
    service.kill(:SIGTERM) if service.running?
  end

  def announcement
    a = {
      :available_memory => @available_memory
    }
    a
  end


  def provision(plan)
    port = @free_ports.first
    @free_ports.delete(port)

    provisioned_service           = ProvisionedService.new
    provisioned_service.name      = "neo4j-#{UUIDTools::UUID.random_create.to_s}"
    provisioned_service.username  = UUIDTools::UUID.random_create.to_s
    provisioned_service.port      = port
    provisioned_service.plan      = plan
    provisioned_service.password  = UUIDTools::UUID.random_create.to_s
    provisioned_service.memory    = @max_memory
    provisioned_service.pid       = start_instance(provisioned_service)

    unless provisioned_service.save
      cleanup_service(provisioned_service)
      raise "Could not save entry: #{provisioned_service.errors.pretty_inspect}"
    end

    response = {
      "hostname" => @local_ip,
      "port" => provisioned_service.port,
      "password" => provisioned_service.password,
      "name" => provisioned_service.name,
      "username" => provisioned_service.username,
    }
    @logger.debug("response: #{response}")
    return response
  rescue => e
    @logger.warn(e)
  end

  def unprovision(name, bindings)
    provisioned_service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if provisioned_service.nil?

    cleanup_service(provisioned_service)
    @logger.debug("Successfully fulfilled unprovision request: #{name}.")
  end

  def cleanup_service(provisioned_service)
    @logger.debug("Killing #{provisioned_service.name} started with pid #{provisioned_service.pid}")

    stop_service(provisioned_service)

    raise "Could not cleanup service: #{provisioned_service.errors.pretty_inspect}" unless provisioned_service.destroy

    Process.kill(9, provisioned_service.pid) if provisioned_service.running?
    dir = File.join(@base_dir, provisioned_service.name)
    
    EM.defer { FileUtils.rm_rf(dir) }

    @available_memory += provisioned_service.memory
    @free_ports << provisioned_service.port

    true
  rescue => e
    @logger.warn(e)
  end

  def bind(name, bind_opts)
    @logger.debug("Bind request: name=#{name}, bind_opts=#{bind_opts}")
    bind_opts ||= BIND_OPT

    provisioned_service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if provisioned_service.nil?

    username = UUIDTools::UUID.random_create.to_s
    password = UUIDTools::UUID.random_create.to_s
    
    ro = bind_opts == "ro"
    r = RestClient.post "http://#{provisioned_service.username}:#{provisioned_service.password}@#{@local_ip}:#{provisioned_service.port}/admin/add-user-#{ro ? 'ro' : 'rw'}","user=#{username}:#{password}"
    raise "Failed to add user:  #{username} status: #{r.code} message: #{r.to_str}" unless r.code == 200
    response = {
      "hostname" => @local_ip,
      "port"    => provisioned_service.port,
      "username" => username,
      "password" => password,
      "name"     => provisioned_service.name,
    }
    $stderr.puts "bind #{name} #{bind_opts} response #{response}"
    @logger.debug("response: #{response}")
    response
  rescue => e
    $stderr.puts "bind #{name} #{bind_opts} exception #{e}"
    @logger.warn(e)
    nil
  end

  def unbind(credentials)
    @logger.debug("Unbind request: credentials=#{credentials}")

    name = credentials['name']
    provisioned_service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if provisioned_service.nil?
    username = credentials['username']
    password = credentials['password']
    r = RestClient.post "http://#{provisioned_service.username}:#{provisioned_service.password}@#{@local_ip}:#{provisioned_service.port}/admin/remove-user", "user=#{username}:#{password}"
    raise "Failed to remove user:  #{username} status: #{r.code} message: #{r.to_str}" unless r.code == 200
    @logger.debug("Successfully unbound #{credentials}")
    true
  rescue => e
    @logger.warn(e)
    nil
  end

  def update_config(dir,provisioned_service) 
    data_dir = File.join(dir, "data","graph.db")
    port = provisioned_service.port
    password = provisioned_service.password
    login = provisioned_service.username
    
    @logger.info("Updating Neo4j in #{dir} with port #{port} admin-login #{login}")
    File.open(File.join(dir, "conf","neo4j-server.properties"), "w") {|f| f.write(@config_template.result(binding))}
  end

  def install_server(dir,provisioned_service)
    @logger.info("Installing Neo4j to #{dir} from #{@neo4j_path} name #{provisioned_service.name}")
    `tar -xz --strip-components=1 -f #{@neo4j_path}/neo4j-server.tgz`
    `rm -rf #{dir}/docs #{dir}/examples`
    `cp #{@neo4j_path}/neo4j-hosting-extension.jar #{dir}/system/lib`
    `cp #{@neo4j_path}/neo4j  #{dir}/bin`
    File.open(File.join(dir, "conf","neo4j.properties"), "a") {|f| f.write("\nenable_remote_shell=false\nenable_online_backup=false\nenable_statistic_collector=false\n")}
  end

  def start_instance(provisioned_service)
    @logger.debug("Starting: #{provisioned_service.pretty_inspect}")

    memory = @max_memory
    name = provisioned_service.name
    dir = File.join(@base_dir, name)
    FileUtils.mkdir_p(dir)

    $0 = "Starting Neo4j service: #{name}"

    
    data_dir = File.join(dir, "data","graph.db")
    
    FileUtils.chdir(dir)
    
    unless File.directory?(data_dir)
        install_server(dir,provisioned_service)
    end
    update_config(dir,provisioned_service)
    
    init_script=File.join(dir,"bin","neo4j")
    @logger.info("Calling #{init_script} start")

    fork do
      handle = IO.popen("#{init_script} start", { close_fds => 1 })
      @logger.debug("Init finished, pid = '#{ handle.pid }' #{ handle }, exit code = #{$? >> 8}")
    end
    Process.wait

    pidfile = File.join(dir,"data","running.pid")
    pid = `[ -f #{pidfile} ] && cat #{pidfile}`
    if pid
      @logger.debug("Service #{name} started with pid #{pid}")
      @available_memory -= memory
      pid = pid.to_i
    end
    pid
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
end
