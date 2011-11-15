# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), '..')
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')
$LOAD_PATH.unshift(File.expand_path("../../../", __FILE__))
$LOAD_PATH.unshift(File.expand_path("../../lib", __FILE__))

require '../../base/spec/spec_helper'

require "rubygems"
require "rspec"
require "socket"
require "timeout"

HTTP_PORT = 9865

def is_port_open?(host, port)
  begin
    Timeout::timeout(1) do
      begin
        s = TCPSocket.new(host, port)
        s.close
        return true
      rescue Errno::ECONNREFUSED, Errno::EHOSTUNREACH => e
        $stderr.puts "Trying to connect to #{host}:#{port} #{e.inspect}"
        return false
      end
    end
  rescue Timeout::Error => e
        $stderr.puts "Trying to connect to #{host}:#{port} #{e.inspect}"
  end
  false
end

def shutdown(sqlfire_node)
    sqlfire_node.shutdown
    $stderr.puts "Shutting down sqlfire-node #{sqlfire_node}"
    sleep 5
    EM.stop
end


def symbolize_keys(hash)
  if hash.is_a? Hash
    new_hash = {}
    hash.each do |k, v|
      new_hash[k.to_sym] = symbolize_keys(v)
    end
    new_hash
  else
    hash
  end
end

def parse_property(hash, key, type, options = {})
  obj = hash[key]
  if obj.nil?
    raise "Missing required option: #{key}" unless options[:optional]
    nil
  elsif type == Range
    raise "Invalid Range object: #{obj}" unless obj.kind_of?(Hash)
    first, last = obj["first"], obj["last"]
    raise "Invalid Range object: #{obj}" unless first.kind_of?(Integer) and last.kind_of?(Integer)
    Range.new(first, last)
  else
    raise "Invalid #{type} object: #{obj}" unless obj.kind_of?(type)
    obj
  end
end

def get_node_config()
  config_file = File.join(File.dirname(__FILE__), "../config/sqlfire_node.yml")
  config = YAML.load_file(config_file)
  sqlfire_props_template = File.join(File.dirname(__FILE__), "../resources/sqlfire.properties.erb")
  options = {
    :logger => Logger.new(parse_property(config, "log_file", String, :optional => true) || STDOUT, "daily"),
    :local_db => 'sqlite3:/tmp/sqlfire/sqlfire_node.db',
    :mbus => parse_property(config, "mbus", String),
    :base_dir => '/tmp/sqlfire/instances',
    :available_memory => parse_property(config, "available_memory", Integer),
    :max_memory => parse_property(config, "max_memory", Integer),
    :node_id => parse_property(config, "node_id", String),
    :sqlfire_home => parse_property(config, "sqlfire_home", String),
    :port_range => parse_property(config, "port_range", Range),
    :config_template => sqlfire_props_template,
  }
  options[:logger].level = Logger::FATAL
  options[:port_range] = (options[:port_range].last+1)..(options[:port_range].last+10)
  options
end

def sqlfire_url(user=@bind_resp['username'],password=@bind_resp['password'],port=@resp['port']) 
  auth = ""
  auth = "#{user}:#{password}@" if user
  "http://#{auth}localhost:#{port}/db/data/"
end

def sqlfire_connect(user=@bind_resp['username'],password=@bind_resp['password'],port=@resp['port']) 
  RestClient.get sqlfire_url(user,password,port) 
end

def get_provisioner_config()
  config_file = File.join(File.dirname(__FILE__), "../config/sqlfire_gateway.yml")
  config = YAML.load_file(config_file)
  config = symbolize_keys(config)
  options = {
    :logger => Logger.new(parse_property(config, "log_file", String, :optional => true) || STDOUT, "daily"),
    # Following options are for Provisioner
    :version => config[:service][:version],
    :local_ip => 'localhost',
    :mbus => config[:mbus],
    # Following options are for AsynchronousServiceGateway
    :service => config[:service],
    :token => config[:token],
    :cloud_controller => config[:cloud_controller],
    # Following options are for Thin
    :host => 'localhost',
    :port => HTTP_PORT
  }
  options[:logger].level = Logger::FATAL
  options
end

def start_server(opts)
  sp = Provisioner.new(@opts).start()
  opts = opts.merge({:provisioner => sp})
  sg = VCAP::Services::AsynchronousServiceGateway.new(opts)
  Thin::Server.start(opts[:host], opts[:port], sg)
  sleep 5
rescue Exception => e
  $stderr.puts e
end




