# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), "..")
$:.unshift File.join(File.dirname(__FILE__), "..", "lib")

ENV["BUNDLE_GEMFILE"] ||= File.expand_path("../../Gemfile", __FILE__)

require "rubygems"
require "rspec"
require "bundler/setup"
require "vcap_services_base"
require "nats/client"
require "vcap/common"
require "datamapper"
require "amqp"
require "thread"
require "rabbit_service/rabbit_node"
require "rabbit_service/rabbit_error"

def getLogger
  logger = Logger.new(STDOUT)
  logger.level = Logger::DEBUG
  logger
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

def config_base_dir
  File.join(File.dirname(__FILE__), "..", "config")
end

def getNodeTestConfig
  config_file = File.join(config_base_dir, "rabbit_node.yml")
  # The configuration file name could be "rabbitmq_node.yml" in dev_setup environment
  if !File.exist?(config_file)
    config_file = File.join(config_base_dir, "rabbitmq_node.yml")
  end
  config = YAML.load_file(config_file)
  options = {
    :logger => getLogger,
    :base_dir => "/tmp/rabbitmq_instances",
    :node_id => parse_property(config, "node_id", String),
    :mbus => parse_property(config, "mbus", String),
    :local_db_file => "/tmp/rabbitmq_node_" + Time.now.to_i.to_s + ".db",
    :ip_route => parse_property(config, "ip_route", String, :optional => true),
    :plan => parse_property(config, "plan", String),
    :capacity => parse_property(config, "capacity", Integer),
    :max_clients => parse_property(config, "max_clients", Integer, :optional => true),
    :port_range => parse_property(config, "port_range", Range),
    :service_log_dir => "/tmp/rabbitmq_instances/log",
    :image_dir => "/tmp/redis_image",
    :max_disk => parse_property(config, "max_disk", Integer),
    :migration_nfs => "/tmp/migration",
    :supported_versions => parse_property(config, "supported_versions", Array),
    :default_version => parse_property(config, "default_version", String),
    :service_start_timeout => parse_property(config, "service_start_timeout", Integer, :optional => true),
    :vm_memory_high_watermark => parse_property(config, "vm_memory_high_watermark", Float, :optional => true),
    :bandwidth_per_second => parse_property(config, "bandwidth_per_second", Float),
  }
  proxy_config = parse_property(config, "bandwidth_proxy", Hash)
  options[:proxy_bin] = parse_property(proxy_config, "bin", String)
  options[:proxy_window] = parse_property(proxy_config, "window", Integer)
  options[:proxy_limit] = parse_property(proxy_config, "size", Integer)
  options[:local_db] = "sqlite3:" + options[:local_db_file]
  options[:rabbit] = parse_property(config, "rabbit", Hash)
  options
end

def amqp_start(credentials, instance)
  result = false
  AMQP.start(:host => instance.ip,
             :port => 10001,
             :vhost => credentials["vhost"],
             :user => credentials["user"],
             :pass => credentials["pass"]) do |conn|
    result = conn.connected?
    AMQP.stop {EM.stop}
  end
  result
end

def amqp_connect(credentials, instance)
  EM.run do
    AMQP.connect(:host => instance.ip,
               :port => 10001,
               :vhost => credentials["vhost"],
               :user => credentials["user"],
               :pass => credentials["pass"])
  end
end
