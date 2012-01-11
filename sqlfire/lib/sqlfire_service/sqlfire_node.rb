# Copyright (c) 2009-2011 VMware, Inc.
require "digest"
require "erb"
require "fileutils"
require "logger"
require "pp"
require "set"

require 'vcap/common'
require 'vcap/component'
require "sqlfire_service/common"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..', 'base', 'lib')
require 'base/node'
require 'base/service_error'
require "datamapper_l"

module VCAP
  module Services
    module Sqlfire
      class Node < VCAP::Services::Base::Node
      end
    end
  end
end

class VCAP::Services::Sqlfire::Node

  include VCAP::Services::Sqlfire::Common

  # FIXME only support rw currently
  BIND_OPT = 'rw'

  class ProvisionedService
    include DataMapper::Resource
    property :name,       String,   :key => true
    property :user,       String,   :required => true
    property :password,   String,   :required => true
    property :netserver_port, Integer,  :unique => true
    property :locator_port, Integer
    property :plan,       String, :required => true
    property :locator,    String
    property :sqlfire_pid, Integer
    property :locator_pid, Integer
    property :memory,     Integer

    def listening?
      begin
        TCPSocket.open('localhost', netserver_port).close
        return true
      rescue => e
        return false
      end
    end


    def running?
      if locator_pid != 0
        VCAP.process_running?(locator_pid) && VCAP.process_running?(sqlfire_pid)
      else
        VCAP.process_running?(sqlfire_pid)
      end
    end


    def kill(sig=9)
      Process.kill(sig, locator_pid) if locator_pid != 0
      Process.kill(sig, sqlfire_pid)
    end
  end


  def initialize(options)
    super(options)
    @base_dir = options[:base_dir]
    FileUtils.mkdir_p(@base_dir)
    @sqlfire_home = options[:sqlfire_home]

    # This is so that the sqlf script is able to find java
    if options[:java_home] && ! options[:java_home].empty?
      ENV["SQLF_JAVA"] = "#{options[:java_home]}/bin/java"
      @logger.info("Setting SQLF_JAVA env variable to #{ENV['SQLF_JAVA']}")
    else
      @logger.info("java_home option not set - will use default on PATH")
    end

    @available_memory = options[:available_memory]
    @max_memory = options[:max_memory]

    @config_template = ERB.new(File.read(options[:config_template]))

    DataMapper.setup(:default, options[:local_db])
    DataMapper::auto_upgrade!

    @free_ports = Set.new
    options[:port_range].each {|port| @free_ports << port}

    # Used to protect instance variables:
    #   @available_memory
    #   @free_ports
    @mutex = Mutex.new

    ProvisionedService.all.each do |provisioned_service|
      fetch_port(provisioned_service.netserver_port)
      if provisioned_service.listening?
        @logger.info("Service #{provisioned_service.name} already listening on port #{provisioned_service.netserver_port}")
        dec_memory(provisioned_service.memory || @max_memory)
        next
      end
      begin
        l_pid, c_pid = start_instance(provisioned_service)
        provisioned_service.locator_pid = l_pid
        provisioned_service.sqlfire_pid = c_pid
        unless provisioned_service.save
          provisioned_service.kill
          raise "Couldn't save pid (#{c_pid})"
        end
      rescue => e
        @logger.warn("Error starting service #{provisioned_service.name}: #{e}")
      end
    end
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


  def inc_memory(memory)
    @mutex.synchronize do
      @available_memory += memory
    end
  end


  def dec_memory(memory)
    @mutex.synchronize do
      @available_memory -= memory
    end
  end


  def sqlfire_status(dir, type=:server)
    status = nil
    result = %x{#{@sqlfire_home}/bin/sqlf #{type} status -dir=#{dir}}
    if result =~ /status: (.*)/
      status = $1.chomp
    end
    status
  end


  def sqlfire_stop(dir, type)
    if sqlfire_running?(dir, type)
      sqlf_str = "#{@sqlfire_home}/bin/sqlf #{type} stop -dir=#{dir}"
      @logger.debug("Stopping sqlfire #{type}: #{sqlf_str}")
      sqlf_args = sqlf_str.split
      system(*sqlf_args)
    else
      @logger.debug("Stop requested for #{type} but was not found running - (#{dir})")
    end
  end


  def sqlfire_running?(dir, type)
    sqlfire_status(dir, type) == "running"
  end


  def sqlfire_stopped?(dir, type)
    sqlfire_status(dir, type) == "stopped"
  end


  #
  # Wait for sqlfire to start and return its pid
  #
  def wait_for_sqlfire(dir, type, spins, quiet=false)
    pid = nil
    starting = false
    sqlfire_str = "#{@sqlfire_home}/bin/sqlf #{type} status -dir=#{dir}"
    status = nil
    while status != "running" && spins > 0
      status_str = %x{#{sqlfire_str}}
      status_str.chomp!
      @logger.debug(">>> #{status_str}") unless quiet
      if status_str =~ /pid: (\d+) status: (.*)/
        pid = $1.to_i
        status = $2.chomp
        case status
        when "running"
          break
        when "starting"
          starting = true
        else
          # If we've already seen 'starting' and don't see 'running' next we can bail out...
          return nil if starting
        end
      end
      spins -= 1
      sleep(1)
    end
    pid
  end


  def shutdown_service(svc)
    sqlfire_stop(dir, :server)
    sqlfire_stop(dir, :locator) if locator.empty?
  end


  def shutdown
    super
    @logger.info("Shutting down instances..")
    ProvisionedService.all.each { |p| shutdown_service(p) }
  end


  def announcement
    services = []
    ProvisionedService.all.each { |ps| services << ps.name }
    { :available_memory => @available_memory, :services => services }
  end


  # Overridden from VCAP::Services::Base::Node
  def on_provision(msg, reply)
    @logger.debug("#{service_description}: Provision request: #{msg} from #{reply}")
    response = ProvisionResponse.new
    prov_req = ProvisionRequest.decode(msg)
    credential = provision(prov_req.name, prov_req.plan, prov_req.options)
    credential['node_id'] = @node_id
    response.credentials = credential
    @logger.debug("#{service_description}: Successfully provisioned service for request #{msg}: #{response.inspect}")
    @node_nats.publish(reply, encode_success(response))
  rescue => e
    @logger.warn(e)
    @node_nats.publish(reply, encode_failure(response, e))
  end


  def provision(name, plan, options={})
    port = fetch_port

    locator_port = nil
    if options["locator"].empty?
      locator_port = fetch_port
    end

    provisioned_service             = ProvisionedService.new
    provisioned_service.name        = name
    provisioned_service.user        = options["user"]
    provisioned_service.password    = options["password"]
    provisioned_service.netserver_port = port
    provisioned_service.locator_port = locator_port
    provisioned_service.plan        = plan
    provisioned_service.memory      = @max_memory
    provisioned_service.locator     = options["locator"]

    provisioned_service.locator_pid, provisioned_service.sqlfire_pid = start_instance(provisioned_service)

    unless provisioned_service.save
      cleanup_service(provisioned_service)
      raise "Could not save entry: #{provisioned_service.errors.pretty_inspect}"
    end

    response = {
      "user"     => provisioned_service.user,
      "password" => provisioned_service.password,
      "hostname" => @local_ip,
      "port" => provisioned_service.netserver_port,
      "locator_port" => provisioned_service.locator_port,
      "name" => provisioned_service.name,
    }
    @logger.debug("response: #{response}")
    return response
  rescue => e
    @logger.warn(e)
    raise "Unable to provision service: #{e}"
  end


  def unprovision(name, credentials)
    provisioned_service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if provisioned_service.nil?

    cleanup_service(provisioned_service)
    @logger.debug("Successfully fulfilled unprovision request: #{name}.")
  end


  def cleanup_service(provisioned_service)
    @logger.debug("Shutting down #{provisioned_service.name} sqlfire")
    raise "Could not cleanup service: #{provisioned_service.errors.pretty_inspect}" unless provisioned_service.destroy

    dir = File.join(@base_dir, provisioned_service.name)
    status = sqlfire_status(dir)
    @logger.debug("Status of #{provisioned_service.name}: #{status}")

    sqlfire_stop(dir, :server)
    sqlfire_stop(dir, :locator)

    EM.defer { FileUtils.rm_rf(dir) }

    inc_memory(provisioned_service.memory)
    return_port(provisioned_service.netserver_port)

    true
  rescue => e
    @logger.warn(e)
  end


  def bind(name, bind_opts, credentials=nil)
    @logger.debug("Bind request: name=#{name}, bind_opts=#{bind_opts}")
    bind_opts ||= BIND_OPT

    provisioned_service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if provisioned_service.nil?

    response = {
      "user"     => provisioned_service.user,
      "password" => provisioned_service.password,
      "hostname" => @local_ip,
      "port"     => provisioned_service.netserver_port,
      "locator_port" => provisioned_service.locator_port,
      "name"     => provisioned_service.name,
    }

    @logger.debug("response: #{response}")
    response
  rescue => e
    @logger.warn(e)
    nil
  end


  def unbind(credentials)
    @logger.debug("Unbind request: credentials=#{credentials}")

    name = credentials['name']
    provisioned_service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if provisioned_service.nil?

    @logger.debug("Successfully unbind #{credentials}")
  rescue => e
    @logger.warn(e)
    nil
  end


  def start_instance(provisioned_service)
    @logger.debug("Starting: #{provisioned_service.pretty_inspect}")

    # Locator string is of the form <host>[<port>]. If the locator has a value
    # then it means we're not starting a locator on *this* host but we need to
    # connect to the locator at the given address.
    # In addition, if we're starting a locator on this host, we need an
    # additional port on which the server will listen,
    if provisioned_service.locator.empty?
      locator_client_port = provisioned_service.netserver_port
      server_client_port = fetch_port
    elsif provisioned_service.locator =~ /(.*)\[(\d+)\]/
      host, port = $1, $2
      wait_for_port_listening(host, port)
      server_client_port = provisioned_service.netserver_port
    else
      raise "Invalid locator value: '#{provisioned_service.locator}'"
    end

    memory = @max_memory
    dir = File.join(@base_dir, provisioned_service.name)

    pid = fork
    if pid
      @logger.debug("Service #{provisioned_service.name} starting")
      dec_memory(memory)
      # In parent, detach the child.
      Process.detach(pid)

      # Unfortunately the server forks itself a couple more times so we need
      # to spin here, polling until things stabilize and we get a pid that's
      # useful.

      # If the locator value is empty we need to start a locator on this host.
      if provisioned_service.locator.empty?
        l_pid = wait_for_sqlfire(dir, :locator, 20)
        @logger.info("Service #{provisioned_service.name} locator pid #{l_pid}")
      else
        # Not starting a locator means we don't have a pid.
        l_pid = 0
      end

      c_pid = wait_for_sqlfire(dir, :server, 15)
      raise "Could not determine sqlfire pid" unless c_pid
      @logger.info("Service #{provisioned_service.name} sqlfire pid #{c_pid}")

      [l_pid, c_pid]
    else
      $0 = "Starting Sqlfire service: #{provisioned_service.name}"
      close_fds

      # Seemingly redundant, these vars get used when parsing the properties template.
      user = provisioned_service.user
      clear_password = provisioned_service.password
      sha_password = sqlf_encrypt_password(provisioned_service.password)

      # Process the properties file
      config = @config_template.result(binding)
      properties_file = File.join(dir, "sqlfire.properties")
      sqlf_ser = File.join(dir, ".sqlfserver.ser")
      locator_ser = File.join(dir, ".sqlflocator.ser")
      locator_disk_store = "locator-disk-store"

      FileUtils.mkdir_p(dir)
      FileUtils.mkdir_p(File.join(dir, locator_disk_store))
      FileUtils.rm_f(properties_file)
      FileUtils.rm_f(sqlf_ser)
      FileUtils.rm_f(locator_ser)

      File.open(properties_file, "w") {|f| f.write(config)}

      if provisioned_service.locator.empty?
        locator_cmd_str = "#{@sqlfire_home}/bin/sqlf \
locator start \
-J-Dsqlfire.properties=#{properties_file} \
-dir=#{dir} -peer-discovery-address=#{@local_ip} \
-peer-discovery-port=#{provisioned_service.locator_port} \
-sys-disk-dir=#{locator_disk_store} \
-run-netserver=true -client-port=#{locator_client_port} \
-client-bind-address=0.0.0.0 -user=#{user}"

        @logger.debug("Starting locator: #{locator_cmd_str}")
        locator_args = locator_cmd_str.split
        system(*locator_args)

        wait_for_sqlfire(dir, :locator, 20, true)
        locator_str = "-locators=#{@local_ip}[#{provisioned_service.locator_port}]"
      else
        locator_str = "-locators=#{provisioned_service.locator}"
      end

      sqlfire_str = "#{@sqlfire_home}/bin/sqlf \
server start \
-J-Dsqlfire.properties=#{properties_file} \
#{locator_str} \
-dir=#{dir} \
-run-netserver=true \
-client-port=#{server_client_port} \
-user=#{user} \
-bind-address=#{@local_ip}"

      @logger.debug("Starting sqlfire: #{sqlfire_str}")
      sqlfire_args = sqlfire_str.split
      exec(*sqlfire_args)
    end
  end


  # Produce the equivalent of running 'sqlf encrypt-password'. Note that bug
  # #43894 will affect this implementation.
  def sqlf_encrypt_password(clear_password)
    hex = to_hex_byte(clear_password)
    sha1 = Digest::SHA1.hexdigest(hex)
    return "3b60" + sha1
  end


  # Broken implementation to retain sqlfire compatibility - see 43894
  def to_hex_byte(clear_password)
    x = []
    (1..(clear_password.size * 2)).each { x << 0 }

    i = 0
    clear_password.bytes.each do |b|
      high = b >> 4
      low = b & 0x0f
      x[i] = high
      x[i+1] = low
      i += 1
    end

    hex_str = ""
    x.each { |b| hex_str << b }
    return hex_str
  end


  def wait_for_port_listening(host, port)
    locator_spins = 10
    while locator_spins > 0
      begin
        TCPSocket.open(host, port).close
        return true
      rescue => e
      end
      sleep 1
      locator_spins -= 1
    end
    false
  end


  def close_fds
    4.upto(get_max_open_fd) do |fd|
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
