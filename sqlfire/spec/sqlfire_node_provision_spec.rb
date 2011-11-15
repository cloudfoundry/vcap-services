# Copyright (c) 2009-2011 VMware, Inc.
require "spec_helper"
require "sqlfire_service/sqlfire_node"
#require "rest-client"


include VCAP::Services::Sqlfire


module VCAP
  module Services
    module Sqlfire
      class Node
        attr_reader :available_memory
      end
    end
  end
end

describe VCAP::Services::Sqlfire::Node do

  before :all do
    EM.run do
      @opts = get_node_config()
      @logger = @opts[:logger]
      @node = Node.new(@opts)
      @original_memory = @node.available_memory

      @resp = @node.provision("sqlfire-test", "free", 
          {"user"=>"foo", "password"=>"granted", "locator"=>""})
      sleep 1
      EM.stop
    end
  end

  after :all do
    EM.run do
      EM.add_timer(1) { EM.stop }
      begin
#        @node.unprovision(@resp["name"], nil)
        EM.stop
      rescue
      end
    end
  end

  it "should have valid response" do
    @resp.should_not be_nil
    inst_name = @resp["name"]
    inst_name.should_not be_nil
    inst_name.should_not == ""
  end

  it "should consume node's memory" do
    (@original_memory - @node.available_memory).should > 0
  end

  it "should be able to connect to sqlfire" do
    is_port_open?('127.0.0.1', @resp['port']).should be_true
  end

  # unprovision here
  it "should be able to unprovision an existing instance" do
    EM.run do
      @node.unprovision(@resp['name'], [])
      e = nil
      begin
        sqlfire_connect(nil,nil);
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
