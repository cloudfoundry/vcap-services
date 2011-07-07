# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require "spec_helper"

describe "mongodb_node provision" do

  before :all do
    EM.run do
      @opts = get_node_config()
      @logger = @opts[:logger]
      @node = Node.new(@opts)
      @original_memory = @node.available_memory

      @resp = @node.provision("free")

      EM.add_timer(1) do
        EM.stop
      end
    end
  end

  it "should have valid response" do
    @resp.should_not be_nil
    puts @resp
    inst_name = @resp['name']
    inst_name.should_not be_nil
    inst_name.should_not == ""
  end

  it "should have a correct size of loopback fs (run as root)" do
    pending "didn't run because u r not root :P" if Process.uid != 0

    size = disk_size(@resp['name'])
    size.should < @opts[:max_space] * 1.05
    size.should > @opts[:max_space] * 0.95
  end

  it "should consume node's memory" do
    (@original_memory - @node.available_memory).should > 0
  end

  it "should be able to connect to mongodb" do
    is_port_open?('127.0.0.1', @resp['port']).should be_true
  end

  it "should not allow unauthorized user to access the instance" do
    EM.run do
      conn = Mongo::Connection.new('localhost', @resp['port']).db(@resp['db'])
      begin
        coll = conn.collection('mongo_unit_test')
        coll.insert({'a' => 1})
        coll.count()
      rescue Exception => e
        @logger.debug e
      end
      e.should_not be_nil
      EM.stop
    end
  end

  it "should return varz" do
    EM.run do
      stats = @node.varz_details
      stats.should_not be_nil
      stats[:running_services].length.should > 0
      stats[:running_services][0]['name'].should_not be_nil
      stats[:running_services][0]['db'].should_not be_nil
      stats[:disk].should_not be_nil
      stats[:services_max_memory].should > 0
      stats[:services_used_memory].should > 0
      EM.stop
    end
  end

  it "should allow authorized user to access the instance" do
    EM.run do
      conn = Mongo::Connection.new('localhost', @resp['port']).db(@resp['db'])
      auth = conn.authenticate(@resp['username'], @resp['password'])
      auth.should be_true
      coll = conn.collection('mongo_unit_test')
      coll.insert({'a' => 1})
      coll.count().should == 1
      EM.stop
    end
  end

  it "should keep the result after node restart" do
    port_open_1 = nil
    port_open_2 = nil
    EM.run do
      EM.add_timer(0) { @node.shutdown }
      EM.add_timer(1) { port_open_1 = is_port_open?('127.0.0.1', @resp['port']) }
      EM.add_timer(2) { @node = Node.new(@opts) }
      EM.add_timer(3) { port_open_2 = is_port_open?('127.0.0.1', @resp['port']) }
      EM.add_timer(4) { EM.stop }
    end

    port_open_1.should be_false
    port_open_2.should be_true
    conn = Mongo::Connection.new('localhost', @resp['port']).db(@resp['db'])
    auth = conn.authenticate(@resp['username'], @resp['password'])
    auth.should be_true
    coll = conn.collection('mongo_unit_test')
    coll.count().should == 1
  end

  it "should return error when unprovisioning a non-existed instance" do
    EM.run do
      e = nil
      begin
        @node.unprovision('no existed', [])
      rescue => e
      end
      e.should_not be_nil
      EM.stop
    end
  end

  # unprovision here
  it "should be able to unprovision an existing instance" do
    EM.run do
      @node.unprovision(@resp['name'], [])

      e = nil
      begin
        conn = Mongo::Connection.new('localhost', @resp['port']).db('db')
      rescue => e
      end
      e.should_not be_nil
      EM.stop
    end
  end

  it "should release memory" do
    EM.run do
      @original_memory.should == @node.available_memory
      EM.stop
    end
  end

end


