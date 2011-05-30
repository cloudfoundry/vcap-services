#!/usr/bin/env ruby
ENV["BUNDLE_GEMFILE"] ||= File.expand_path("../Gemfile", __FILE__)
require "rubygems"
require "bundler/setup"
require 'sinatra'
require 'thin'
require 'datamapper'
require 'logger'
require 'yajl'

# VCAP environment
port = ENV['VMC_APP_PORT']
port ||= 8082

class TestApp < Sinatra::Base
  set :public, File.join(File.dirname(__FILE__) , '/static')
  set :views, File.join(File.dirname(__FILE__) , '/template')

  class User
    include DataMapper::Resource
    property :id, Serial, :key => true
    property :name, String, :required => true
  end

  def initialize(opts)
    super
    @opts = opts
    @logger = Logger.new(STDOUT, 'daily')
    @logger.level = Logger::DEBUG
    DataMapper.setup(:default, @opts[:database])
    DataMapper::auto_upgrade!
  end

  not_found do
    404
  end

  error do
    @logger.error("Error: #{env['sinatra.erro']}")
  end

  get '/' do
    'It works.'
  end

  get '/user/:id' do
    @logger.debug("Get user #{params[:id]}")
    user = User.get(params[:id])
    user.name
  end

  get '/user' do
    users = User.all
    res = ""
    users.each do |user|
      name = user.name
      res += "#{name}\n"
    end
    res
  end

  post '/user/:name' do
    @logger.debug("Create a user #{params[:name]}")
    user = User.new
    user.name = params[:name]
    if not user.save
      @logger.error("Can't save to db:#{user.errors.pretty_inspect}")
      500
    else
      redirect ("/user/#{user.id}")
    end
  end

end

config_file = File.expand_path("../config/default.yml", __FILE__)
begin
  config = YAML.load_file(config_file)
rescue => e
  puts "Could not read configuration file:  #{e}"
  exit
end

def symbolize_keys(hash)
  if hash.is_a? Hash
    new_hash = {}
    hash.each {|k, v| new_hash[k.to_sym] = symbolize_keys(v)}
    new_hash
  else
    hash
  end
end
new_config = symbolize_keys(config)

svcs = ENV['VMC_SERVICES']
if svcs
  # override db config if VMC_SERVICE atmos service is supplied.
  svcs = Yajl::Parser.parse(svcs)
  svcs.each do |svc|
    if svc["name"] =~ /^mysql/
      opts = svc["options"]
      user,passwd,host,db,db_port = %w(user password hostname name port).map {|key|
        opts[key]}
      conn_string="mysql://#{user}:#{passwd}@#{host}:#{db_port}/#{db}"
      new_config[:database] = conn_string
    end
  end
end

puts "Config: #{new_config.inspect}"
instance = TestApp.new(new_config)
Thin::Server.start('0.0.0.0', port , instance)
