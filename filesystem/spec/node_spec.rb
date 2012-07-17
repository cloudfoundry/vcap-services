# Copyright (c) 2009-2012 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), "..", "lib")
require File.dirname(__FILE__) + "/spec_helper"
require "filesystem_service/nfs_node"
require "filesystem_service/local_node"
require "filesystem_service/error"

class VCAP::Services::Filesystem::NFSNode
  attr_accessor :rclient, :req_id, :usage_id
end

describe VCAP::Services::Filesystem::NFSNode do

  before :all do
    @options = getNodeTestConfig
    @options.freeze
    # Setup code must be wrapped in EM.run
    EM.run do
      @node = VCAP::Services::Filesystem::NFSNode.new(@options)
      EM.add_timer(1){ EM.stop }
    end
  end

  after :all do
    FileUtils.rm_rf("/tmp/fss")
  end

  describe "Basic Operations" do
    it "should be able to provision instance" do
      EM.run do
        # Provision a new instance
        cred = @node.provision("free")
        sid  = cred["internal"]["name"]
        backend = @node.get_backend(cred)
        path = @node.get_instance_dir(sid, backend)
        Dir.exist?(path).should be_true
        instance = VCAP::Services::Filesystem::NFSNode::ProvisionedService.get(sid)
        instance.name.should == sid

        #Clean up
        @node.unprovision(sid)

        # Provision a instance by credential
        cred_new = @node.provision("free", cred)
        cred_new.should == cred
        Dir.exist?(path).should be_true
        instance = VCAP::Services::Filesystem::NFSNode::ProvisionedService.get(sid)
        instance.name.should == sid

        EM.stop
      end
    end

    it "should be able to unprovision instance" do
      EM.run do
        cred = @node.provision("free")
        sid  = cred["internal"]["name"]
        backend = @node.get_backend(cred)
        path = @node.get_instance_dir(sid, backend)
        @node.unprovision(sid)
        Dir.exist?(path).should be_false
        instance = VCAP::Services::Filesystem::NFSNode::ProvisionedService.get(sid)
        instance.should be_nil
        EM.stop
      end
    end

    it "should be able to bind/unbind" do
      EM.run do
        cred = @node.provision("free")
        sid = cred["internal"]["name"]
        bind = @node.bind(sid)
        bind["internal"]["name"].should == sid
        @node.unbind(bind).should == {}
        bind_new = @node.bind(sid, {}, bind)
        bind_new.should == bind
        EM.stop
      end
    end

    it "should be able to handle error" do
      [
        ["FileUtils", "mkdir", VCAP::Services::Filesystem::FilesystemError, /Could not create instance directory/],
        ["FileUtils", "chmod", VCAP::Services::Filesystem::FilesystemError, /Could not change access permission/],
        ["VCAP::Services::Filesystem::NFSNode::ProvisionedService.any_instance", "save",
         VCAP::Services::Filesystem::FilesystemError, /Could not save instance/],
      ].each do |klass, method, *error|
        error_stub(klass, method) do
          expect { @node.provision("free") }.to raise_error(*error)
        end
      end
    end
  end

  describe "Handle redis job" do
    it "should serve du job from redis" do
      EM.run do
        cred = @node.provision("free")
        sid  = cred["internal"]["name"]
        @node.rclient.hset(@node.req_id, sid, "true")
        @node.rclient.should_receive(:hdel).with(@node.req_id, sid)
        @node.rclient.should_receive(:hset).with(@node.usage_id, sid, anything)
        @node.serve_redis
        EM.add_timer(1){ EM.stop }
      end
    end

    it "should handle error" do
      [
        ["VCAP::Services::Filesystem::NFSNode.any_instance", "get_instance"],
        ["Redis.any_instance", "multi"],
        ["Redis.any_instance", "hgetall"],
      ].each do |klass, method|
        error_stub(klass, method) do
          expect do
            EM.run do
              cred = @node.provision("free")
              sid  = cred["internal"]["name"]
              @node.rclient.hset(@node.req_id, sid, "true")
              @node.serve_redis
              EM.add_timer(1){ EM.stop }
            end
          end.to_not raise_error
        end
      end
    end
  end
end

describe VCAP::Services::Filesystem::LocalNode do
  before :all do
    @options = getNodeTestConfig("local")
    @options.freeze
    # Setup code must be wrapped in EM.run
    EM.run do
      @node = VCAP::Services::Filesystem::LocalNode.new(@options)
      EM.add_timer(1){ EM.stop }
    end
  end

  after :all do
    FileUtils.rm_rf("/tmp/fss")
  end

  it "should support basic ops" do
    EM.run do
      cred = @node.provision("free")
      sid  = cred["internal"]["name"]
      backend = @node.get_backend(cred)
      path = @node.get_instance_dir(sid, backend)
      Dir.exist?(path).should be_true
      instance = VCAP::Services::Filesystem::NFSNode::ProvisionedService.get(sid)
      instance.name.should == sid
      bind = @node.bind(sid)
      bind["internal"]["name"].should == sid
      @node.unbind(bind).should == {}
      @node.unprovision(sid)
      Dir.exist?(path).should be_false
      instance = VCAP::Services::Filesystem::NFSNode::ProvisionedService.get(sid)
      instance.should be_nil
      EM.stop
    end
  end
end
