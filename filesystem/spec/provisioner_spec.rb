# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), "..", "lib")
$LOAD_PATH.unshift(File.expand_path("../../../base/lib", __FILE__))

require File.dirname(__FILE__) + "/spec_helper"
require "filesystem_service/provisioner"

module VCAP
  module Services
    module Filesystem
      class Provisioner
         attr_accessor :logger, :backends
      end
    end
  end
end

describe VCAP::Services::Filesystem::Provisioner do

  before :all do
    @logger = Logger.new(STDOUT, "daily")
    @logger.level = Logger::DEBUG
    backends = [
      {
        "host" => "10.0.0.1",
        "export" => "backend1",
        "mount" => "/tmp/backend1"
      },
      {
        "host" => "10.0.0.2",
        "export" => "backend2",
        "mount" => "/tmp/backend2"
      },
      {
        "host" => "10.0.0.3",
        "export" => "backend3",
        "mount" => "/tmp/backend3"
      }
    ]
    @options = {
      :logger => @logger,
      :ip_route => "127.0.0.1",
      :additional_options => {
        :backends => backends
      }
    }
    EM.run do
      @provisioner = VCAP::Services::Filesystem::Provisioner.new(@options)
      EM.add_timer(1) {EM.stop}
    end
    FileUtils.mkdir_p("/tmp/backend1")
    FileUtils.mkdir_p("/tmp/backend2")
    FileUtils.mkdir_p("/tmp/backend3")
  end

  after :all do
    FileUtils.rm_rf("/tmp/backend1")
    FileUtils.rm_rf("/tmp/backend2")
    FileUtils.rm_rf("/tmp/backend3")
  end

  describe 'Provisioner.provision' do
    before :all do
      @request = VCAP::Services::Internal::ProvisionRequest.new
      @request.plan = "free"
    end

    it "should return the credentials when provision successful" do
      @provisioner.provision_service(@request) do |msg|
        msg["success"].should == true
        msg["response"][:data][:plan].should == "free"
        msg["response"][:service_id].should be
        msg["response"][:credentials]["internal"].should be
        msg["response"][:credentials]["internal"]["name"].should be
        msg["response"][:credentials]["internal"]["host"].should be
        msg["response"][:credentials]["internal"]["export"].should be
        @provisioner.unprovision_service(msg["response"][:service_id]) {}
      end
    end

    it "should create instance directory when provision successful" do
      @provisioner.provision_service(@request) do |msg|
        backend = @provisioner.get_backend(msg["response"][:credentials]["internal"]["host"], msg["response"][:credentials]["internal"]["export"])
        File.exists?(File.join(backend["mount"], msg["response"][:service_id])).should == true
        @provisioner.unprovision_service(msg["response"][:service_id]) {}
      end
    end

    it "should return error when no backend found" do
      backends = @provisioner.backends
      @provisioner.backends = nil
        @provisioner.provision_service(@request) do |msg|
          msg["success"].should == false
          msg["response"]["msg"]["code"].should == VCAP::Services::Filesystem::FilesystemError::FILESYSTEM_GET_BACKEND_FAILED[0]
        end
      @provisioner.backends = backends
    end

    it "should return error when create instance directory failed" do
      backends = @provisioner.backends
      @provisioner.backends = [
        {
          "host" => "10.0.0.1",
          "export" => "backend1",
          "mount" => "/tmp/non_exsit1"
        },
        {
          "host" => "10.0.0.2",
          "export" => "backend2",
          "mount" => "/tmp/non_exsit2"
        },
        {
          "host" => "10.0.0.3",
          "export" => "backend3",
          "mount" => "/tmp/non_exsit3"
        },
      ]
      @provisioner.provision_service(@request) do |msg|
        msg["success"].should == false
        msg["response"]["msg"]["code"].should == VCAP::Services::Filesystem::FilesystemError::FILESYSTEM_CREATE_INSTANCE_DIR_FAILED[0]
      end
      @provisioner.backends = backends
    end
  end

  describe 'Provisioner.unprovision' do
    before :all do
      @request = VCAP::Services::Internal::ProvisionRequest.new
      @request.plan = "free"
    end

    it "should return true when unprovision successful" do
      @provisioner.provision_service(@request) do |prov_msg|
        @provisioner.unprovision_service(prov_msg["response"][:service_id]) do |msg|
          msg["success"].should == true
          msg["response"].should == true
        end
      end
    end

    it "should delete the instance directory when unprovision successful" do
      @provisioner.provision_service(@request) do |prov_msg|
        backend = @provisioner.get_backend(prov_msg["response"][:credentials]["internal"]["host"], prov_msg["response"][:credentials]["internal"]["export"])
        @provisioner.unprovision_service(prov_msg["response"][:service_id]) do |msg|
          File.exists?(File.join(backend["mount"], prov_msg["response"][:service_id])).should == false
        end
      end
    end

    it "should return error when unprovision a non-existed instance" do
      @provisioner.unprovision_service("non-existed") do |msg|
        msg["success"].should == false
        msg["response"]["msg"]["code"].should == VCAP::Services::Filesystem::FilesystemError::FILESYSTEM_FIND_INSTANCE_FAILED[0]
      end
    end
  end

  describe 'Provisioner.bind' do
    before :all do
      @request = VCAP::Services::Internal::ProvisionRequest.new
      @request.plan = "free"
    end

    it "should return the credentials when bind successful" do
      @provisioner.provision_service(@request) do |prov_msg|
        @provisioner.bind_instance(prov_msg["response"][:service_id], nil) do |msg|
          msg["success"].should == true
          msg["response"][:configuration].should be
          msg["response"][:service_id].should be
          msg["response"][:credentials]["internal"].should be
        end
        @provisioner.unprovision_service(prov_msg["response"][:service_id]) {}
      end
    end

    it "should return error when bind a non-existed instance" do
      @provisioner.bind_instance("non-existed", nil) do |msg|
        msg["success"].should == false
        msg["response"]["msg"]["code"].should == VCAP::Services::Filesystem::FilesystemError::FILESYSTEM_FIND_INSTANCE_FAILED[0]
      end
    end
  end

  describe 'Provisioner.unbind' do
    before :all do
      @request = VCAP::Services::Internal::ProvisionRequest.new
      @request.plan = "free"
    end

    it "should return true when unbind successful" do
      @provisioner.provision_service(@request) do |prov_msg|
        @provisioner.bind_instance(prov_msg["response"][:service_id], nil) do |bind_msg|
          @provisioner.unprovision_service(bind_msg["response"][:service_id]) do |msg|
            msg["success"].should == true
            msg["response"].should == true
          end
        end
        @provisioner.unprovision_service(prov_msg["response"][:service_id]) {}
      end
    end
  end
end
