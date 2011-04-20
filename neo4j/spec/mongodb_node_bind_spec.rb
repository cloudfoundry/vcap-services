# Copyright (c) 2009-2011 VMware, Inc.
require "spec_helper"
require "neo4j_service/neo4j_node"
require "neo4j"

include VCAP::Services::Neo4j


module VCAP
  module Services
    module Neo4j
      class Node
        attr_reader :available_memory
      end
    end
  end
end

describe VCAP::Services::Neo4j::Node do

  before :all do
    EM.run do
      @app_id = "myapp"
      @opts = get_node_config()
      @logger = @opts[:logger]

      @node = Node.new(@opts)
      @resp = @node.provision("free")
      sleep 1

      @bind_resp = @node.bind(@resp['name'], 'rw')
      sleep 1

      EM.stop
    end
  end

  it "should have valid response" do
    @bind_resp.should_not be_nil
    @bind_resp['hostname'].should_not be_nil
    @bind_resp['port'].should_not be_nil
    @bind_resp['username'].should_not be_nil
    @bind_resp['password'].should_not be_nil
  end

  it "should be able to connect to neo4j" do
    is_port_open?('127.0.0.1', @resp['port']).should be_true
  end

  it "should allow authorized user to access the instance" do
    EM.run do
      conn = Neo4j::Connection.new('localhost', @resp['port']).db(@resp['db'])
      auth = conn.authenticate(@bind_resp['username'], @bind_resp['password'])
      auth.should be_true
      coll = conn.collection('neo4j_unit_test')
      coll.insert({'a' => 1})
      coll.find()
      coll.count().should == 1
      EM.stop
    end
  end

  it "should not allow unauthorized user to access the instance" do
    EM.run do
      conn = Neo4j::Connection.new('localhost', @resp['port']).db(@resp['db'])
      begin
        coll = conn.collection('neo4j_unit_test')
        coll.insert({'a' => 1})
        coll.find()
        coll.count().should == 1
      rescue => e
      end
      e.should_not be_nil
      EM.stop
    end
  end

  it "should not allow valid user with empty password to access the instance" do
    EM.run do
      conn = Neo4j::Connection.new('localhost', @resp['port']).db(@resp['db'])
      begin
        coll = conn.collection('neo4j_unit_test')
        auth = conn.authenticate(@bind_resp['login'], '')
        auth.should be_false
        coll.insert({'a' => 1})
        coll.find()
      rescue => e
      end
      e.should_not be_nil
      EM.stop
    end
  end

  # unbind here
  it "should be able to unbind it" do
    EM.run do
      resp  = @node.unbind(@bind_resp)
      resp.should be_true
      sleep 1
      EM.stop
    end
  end

  it "should not allow user to access the instance after unbind" do
    EM.run do
      begin
        conn = Neo4j::Connection.new('localhost', @resp['port']).db(@resp['db'])
        auth = conn.authenticate(@bind_resp['login'], @bind_resp['secret'])
        auth.should be_false
        coll = conn.collection('neo4j_unit_test')
        coll.insert({'a' => 1})
        coll.find()
      rescue => e
        e.should_not be_nil
      end
      EM.stop

    end
  end

  # unprovision here
  it "should be able to unprovision an existing instance" do
    EM.run do
      @node.unprovision(@resp['name'], [])

      e = nil
      begin
        conn = Neo4j::Connection.new('localhost', @resp[:port]).db('local')
      rescue => e
      end
      e.should_not be_nil
      EM.stop
    end
  end

end


