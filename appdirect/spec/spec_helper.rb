# Copyright (c) 2009-2012 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), "..")
$:.unshift File.join(File.dirname(__FILE__), "..", "lib")

ENV["RACK_ENV"] = "test"

require "rubygems"
require "bundler"
Bundler.require(:default, :test)

require "simplecov"
require "simplecov-rcov"
class SimpleCov::Formatter::MergedFormatter
  def format(result)
     SimpleCov::Formatter::HTMLFormatter.new.format(result)
     SimpleCov::Formatter::RcovFormatter.new.format(result)
  end
end
SimpleCov.formatter = SimpleCov::Formatter::MergedFormatter
SimpleCov.start

require "rspec"
require "webmock/rspec"
require "bundler/setup"
#require "vcap_services_base"
require "rack/test"
require "json"
require "logger"
require "yaml"
require "webmock"


include WebMock::API

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "..", "..")
#require "vcap/common"

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

def stub_fixture(verb, api, path, scenario = "")
  url = "#{api}/#{path}"
  fixture = "#{scenario}#{path}/#{verb.to_s}_response.json"
  req_fixture = "#{scenario}#{path}/#{verb.to_s}_request.json"
  stuff = load_fixture(fixture)
  stub_request(verb, url).to_return(:body=> stuff)
  JSON.parse(load_fixture(req_fixture))
end

def load_config()
  config_file = File.join(File.dirname(__FILE__), "..", "config", "appdirect_gateway.yml")
  config = YAML.load_file(config_file)
  config = symbolize_keys(config)
  config[:logger] = make_logger()
  config
end

def make_logger()
  logger = Logger.new(STDOUT)
  logger.level = Logger::ERROR
  logger
end

def load_fixture(filename)
  File.read("#{File.dirname(__FILE__)}/fixtures/#{filename}") rescue "{}"
end

# http://eigenclass.org/hiki/Changes+in+Ruby+1.9#l156
# Default Time.to_s changed in 1.9, monkeypatching it back
class Time
  def to_s
    strftime("%a %b %d %H:%M:%S %Z %Y")
  end
end