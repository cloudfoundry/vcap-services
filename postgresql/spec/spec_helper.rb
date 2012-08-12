# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), '..')
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'rubygems'
require 'rspec'
require 'bundler/setup'
require 'vcap_services_base'

require 'postgresql_service/util'
require 'postgresql_service/provisioner'

require 'postgresql_service/with_warden'
# monkey patch of wardenized node
module VCAP::Services::Postgresql::WithWarden
  alias_method :pre_send_announcement_internal_ori, :pre_send_announcement_internal
  def pre_send_announcement_internal
    unless @use_warden && @options[:not_start_instances]
      pre_send_announcement_internal_ori
    else
      @logger.info("Not to start instances")
    end
  end
end

require 'postgresql_service/node'

require 'pry'
require 'pry-nav'
require 'pry-stack_explorer'

module Boolean;end
class ::TrueClass; include Boolean; end
class ::FalseClass; include Boolean; end

def getLogger()
  logger = Logger.new( STDOUT)
  logger.level = Logger::ERROR
  return logger
end

def connect_to_postgresql(options)
  host, user, password, port, db =  %w{hostname user password port name}.map { |opt| options[opt] }
  PGconn.connect(host, port, nil, nil, db, user, password)
end

def getNodeTestConfig()
  config_file = File.join(File.dirname(__FILE__), "../config/postgresql_node.yml")
  config = YAML.load_file(config_file)
  options = {
    :logger => getLogger,
    :base_dir => parse_property(config, "base_dir", String),
    :plan => parse_property(config, "plan", String),
    :capacity => parse_property(config, "capacity", Integer),
    :max_db_size => parse_property(config, "max_db_size", Integer),
    :max_long_query => parse_property(config, "max_long_query", Integer),
    :node_id => parse_property(config, "node_id", String),
    :mbus => parse_property(config, "mbus", String),
    :local_db => parse_property(config, "local_db", String),
    :postgresql => parse_property(config, "postgresql", Hash),
    :ip_route => parse_property(config, "ip_route", String, :optional => true),
    :max_long_tx => parse_property(config, "max_long_tx", Integer),
    :max_db_conns => parse_property(config, "max_db_conns", Integer),
    :restore_bin => parse_property(config, "restore_bin", String),
    :dump_bin => parse_property(config, "dump_bin", String),
    :db_size_overhead => parse_property(config, "db_size_overhead", Float),
    :disk_overhead => parse_property(config, "disk_overhead", Float, :disk_overhead => 0.0),
    :use_warden => parse_property(config, "use_warden", Boolean, :optional => true, :default => false)
  }
  if options[:use_warden]
    warden_config = parse_property(config, "warden", Hash, :optional => true)
    options[:use_warden] = true
    options[:log_dir] = parse_property(warden_config, "log_dir", String)
    options[:port_range] = parse_property(warden_config, "port_range", Range)
    options[:image_dir] = parse_property(warden_config, "image_dir", String)
    options[:filesystem_quota] = parse_property(warden_config, "filesystem_quota", Boolean, :optional => true)
    options[:service_start_timeout] = parse_property(warden_config, "service_start_timeout", Integer, :optional => true, :default => 3)
  else
    options[:ip_route] = "127.0.0.1"
  end
  options
end

def getProvisionerTestConfig()
  config_file = File.join(File.dirname(__FILE__), "../config/postgresql_gateway.yml")
  config = YAML.load_file(config_file)
  config = VCAP.symbolize_keys(config)
  options = {
    :logger   => getLogger,
    :version  => config[:service][:version],
    :local_ip => config[:host],
    :plan_management => config[:plan_management],
    :mbus => config[:mbus]
  }
  options
end

def parse_property(hash, key, type, options = {})
  obj = hash[key]
  if obj.nil?
    raise "Missing required option: #{key}" unless options[:optional]
    options[:default]
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
