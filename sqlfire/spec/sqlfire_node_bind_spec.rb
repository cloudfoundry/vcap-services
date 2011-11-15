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
      @app_id = "myapp"
      @opts = get_node_config()
      @logger = @opts[:logger]

      @node = Node.new(@opts)
      @resp = @node.provision("sqlfire-test", "free", 
          {"user"=>"foo", "password"=>"granted", "locator"=>""})

      sleep 1
      @bind_resp = @node.bind(@resp["name"], "rw")
      
      sleep 1
      EM.stop
    end
  end

  after :all do
    EM.run do
      EM.add_timer(1) { EM.stop }
      begin
        @node.unprovision(@resp["name"], nil)
        EM.stop
      rescue
      end
    end
  end

  it "should have valid response" do
    @bind_resp.should_not be_nil
    @bind_resp['hostname'].should_not be_nil
    @bind_resp['port'].should_not be_nil
    @bind_resp['name'].should_not be_nil
  end

  it "should be able to connect to sqlfire" do
    is_port_open?('127.0.0.1', @resp['port']).should be_true
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

  # unprovision here
  it "should be able to unprovision an existing instance" do
    EM.run do
      @node.unprovision(@resp['name'], [])

      e = nil
      begin
        sqlfire_connect(nil,nil)
      rescue => e
      end
      e.should_not be_nil
      EM.stop
    end
  end

end


