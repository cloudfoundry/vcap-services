# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require 'spec_helper'
require 'logger'
require 'yajl'
require 'mysql_service/util'
require 'timeout'

describe 'Mysql Connection Pool Test' do

  before :all do
    @opts = getNodeTestConfig
    @opts.freeze
    @mysql_config = @opts[:mysql]
    host, user, password, port, socket =  %w{host user pass port socket}.map { |opt| @mysql_config[opt] }
    @pool = connection_pool_klass.new(:host => host, :username => user, :password => password, :database => "mysql", :port => port.to_i, :socket => socket)

  end

  it "should provide mysql connections" do
    @pool.with_connection do |conn|
      expect {conn.query("select 1")}.should_not raise_exception
    end
  end

  it "should not provide the same connection to different threads" do
    THREADS = 20
    ITERATES = 50
    conns = []
    lock = Mutex.new
    threads = []
    THREADS.times do
      thread  = Thread.new do
        ITERATES.times do
          id = nil
          begin
            @pool.with_connection do |conn|
              id = conn.query("SELECT CONNECTION_ID()").each(:as => :array)[0]
              id.should_not == nil
              conns.include?(id).should == false
              lock.synchronize {conns << id }
            end
          ensure
            lock.synchronize {conns.delete(id) }
          end
        end
      end
      threads << thread
    end
    threads.each {|t| t.join}
  end

  it "should verify a connection before checkout" do
    host, user, password, port, socket =  %w{host user pass port socket}.map { |opt| @mysql_config[opt] }
    pool = connection_pool_klass.new(:host => host, :username => user, :password => password, :database => "mysql", :port => port.to_i, :socket => socket, :pool => 1)

    pool.with_connection do |conn|
      conn.close
    end

    pool.with_connection do |conn|
      expect{conn.query("select 1")}.should_not raise_error
    end
  end
end
