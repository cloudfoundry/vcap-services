# Copyright (c) 2009-2012 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), '..')
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

ENV["BUNDLE_GEMFILE"] ||= File.expand_path("../../Gemfile", __FILE__)

require 'rubygems'
require 'bundler/setup'
require 'vcap_services_base'
require 'rspec/core'
require 'vcap/common'
require "uri"
require 'filesystem_service/error'

TEST_BASE = "/tmp/fss"
NFS_BACKENDS = [
  {
    "host" => "10.0.0.1",
    "export" => "backend1",
    "mount" => "#{TEST_BASE}/backend1"
  },
  {
    "host" => "10.0.0.2",
    "export" => "backend2",
    "mount" => "#{TEST_BASE}/backend2"
  },
  {
    "host" => "10.0.0.3",
    "export" => "backend3",
    "mount" => "#{TEST_BASE}/backend3"
  }
]

LOCAL_BACKENDS = [
  {
    "local_path" => "#{TEST_BASE}/backend1"
  },
  {
    "local_path" => "#{TEST_BASE}/backend2"
  },
  {
    "local_path" => "#{TEST_BASE}/backend3"
  }
]

class MockRedisClient
  def initialize(*args)
    @table = {}
  end

  def hset(key, field, value)
    @table[key] ||= {}
    @table[key][field] = value
  end

  def hget(key, field)
    return nil unless @table[key]
    @table[key][field]
  end

  def hgetall(key)
    @table[key] || {}
  end

  def hdel(key, field)
    @table[key].delete(field) if @table[key]
  end

  def multi
    yield(self) if block_given?
  end

  def method_missing(sym, *args, &block)
    puts "MockRedisClient is called #{sym}"
  end
end

Redis = MockRedisClient

def config_base_dir
  ENV["CLOUD_FOUNDRY_CONFIG_PATH"] || File.join(File.dirname(__FILE__), "..", "config")
end

def getNodeTestConfig(type="nfs")
  logger = Logger.new('/dev/null')
  logger.level = Logger::DEBUG
  config_file = File.join(config_base_dir, "filesystem_node.yml")
  config = YAML.load_file(config_file)
  config = config.inject({}){ |m, (k, v)| m[k.to_sym] = v; m }
  config[:base_dir] = TEST_BASE
  config[:logger]   = logger
  config[:local_db] = "sqlite3:#{TEST_BASE}/fss.db"
  config[:fs_type]  = type
  config[:backends] = eval "#{type}_backends".upcase

  FileUtils.mkdir_p("#{TEST_BASE}/backend1")
  FileUtils.mkdir_p("#{TEST_BASE}/backend2")
  FileUtils.mkdir_p("#{TEST_BASE}/backend3")

  config
end

def error_stub(klass, method)
  eval "#{klass}.stub(:#{method}){ raise 'test error' }"
  yield if block_given?
  eval "#{klass}.unstub(:#{method})"
end
