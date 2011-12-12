# Copyright (c) 2009-2011 VMware, Inc.

PWD = File.dirname(__FILE__)
TMP = '/tmp/mvstore'

$:.unshift File.join(PWD, '..')
$:.unshift File.join(PWD, '..', 'lib')

require "rubygems"
require "rspec"
require "socket"
require "timeout"
require "erb"
require "mvstore_service/mvstore_node"
require "fileutils"

# Define constants
HTTP_PORT = 9865

TEST_COLL    = 'testColl'
TEST_KEY     = 'test_key'
TEST_VAL     = 1234
TEST_VAL_2   = 4321

BACKUP_DIR    = File.join(TMP, 'backup')
CONFIG_DIR    = File.join(TMP, 'config')
CONFIG_FILE   = File.join(TMP, 'mvstore_backup.yml')
TEMPLATE_FILE = File.join(PWD, 'config/mvstore_backup.yml.erb')

FileUtils.mkdir_p(BACKUP_DIR)
FileUtils.mkdir_p(CONFIG_DIR)

include VCAP::Services::MVStore

module VCAP
  module Services
    module MVStore
      class Node
        attr_reader :available_memory
      end
    end
  end
end

def is_port_open?(host, port)
  begin
    Timeout::timeout(1) do
      begin
        s = TCPSocket.new(host, port)
        s.close
        return true
      rescue Errno::ECONNREFUSED, Errno::EHOSTUNREACH
        return false
      end
    end
  rescue Timeout::Error
  end
  false
end

def get_backup_dir(backup_dir)
  dir = backup_dir
  # Backup Dir: base_backup/mvstore/ab/cd/ef/uuid/timestamp
  #             base_backup/<6-more-layers>
  6.times do
    dirs = Dir.entries(dir)
    dirs.delete('.')
    dirs.delete('..')
    dir = File.join(dir, dirs[0])
  end
  dir
end

def delete_admin(options)
  #   db = Mongo::Connection.new('127.0.0.1', options['port']).db(options['db'])
  #   auth = db.authenticate(options['username'], options['password'])
  #   db.remove_user('admin')
  # 
  #   db = Mongo::Connection.new('127.0.0.1', options['port']).db('admin')
  #   service = VCAP::Services::MongoDB::Node::ProvisionedService.get(options['name'])
  #   auth = db.authenticate(service.admin, service.adminpass)
  #   db.remove_user(service.admin)
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
  config_file = File.join(PWD, "../config/mvstore_node.yml")
  config = YAML.load_file(config_file)
  mvstore_conf_template = File.join(PWD, "../resources/mvstore.conf.erb")
  options = {
    :logger => Logger.new(parse_property(config, "log_file", String, :optional => true) || STDOUT, "daily"),
    :mvstore_path => parse_property(config, "mvstore_path", String),
    :mvstore_restore_path => parse_property(config, "mvstore_restore_path", String),
    :ip_route => parse_property(config, "ip_route", String, :optional => true),
    :available_memory => parse_property(config, "available_memory", Integer),
    :node_id => parse_property(config, "node_id", String),
    :mbus => parse_property(config, "mbus", String),
    :config_template => mvstore_conf_template,
    :port_range => parse_property(config, "port_range", Range),
    :max_memory => parse_property(config, "max_memory", Integer),
    :base_dir => '/tmp/mvstore/instances',
    :mvstore_log_dir => '/tmp/mvstore/mvstore_log',
    :local_db => 'sqlite3:/tmp/mvstore/mvstore_node.db'
  }
  options[:logger].level = Logger::FATAL
  options
end
