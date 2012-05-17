# Copyright (c) 2009-2011 VMware, Inc.
require "fileutils"
require "logger"
require "datamapper"
require "uuidtools"

module VCAP
  module Services
    module Echo
      class Node < VCAP::Services::Base::Node
      end
    end
  end
end

require "echo_service/common"
require "echo_service/echo_error"

class VCAP::Services::Echo::Node

  include VCAP::Services::Echo::Common
  include VCAP::Services::Echo

  class ProvisionedService
    include DataMapper::Resource
    property :name,       String,   :key => true
    property :plan,       Integer,  :required => true
  end

  def initialize(options)
    super(options)

    @base_dir = options[:base_dir]
    FileUtils.mkdir_p(@base_dir)
    @local_db = options[:local_db]
    @port = options[:port]
  end

  def pre_send_announcement
    super
    start_db
    @capacity_lock.synchronize do
      ProvisionedService.all.each do |instance|
        @capacity -= capacity_unit
      end
    end
  end

  def announcement
    @capacity_lock.synchronize do
      { :available_capacity => @capacity,
        :capacity_unit => capacity_unit }
    end
  end

  def provision(plan, credentials = nil)
    raise EchoError.new(EchoError::ECHO_INVALID_PLAN, plan) unless plan == @plan
    instance = ProvisionedService.new
    instance.plan = 1
    if credentials
      instance.name = credentials["name"]
    else
      instance.name = UUIDTools::UUID.random_create.to_s
    end

    begin
      save_instance(instance)
    rescue => e1
      @logger.error("Could not save instance, cleanning up")
      begin
        cleanup_instance(instance)
      rescue => e2
        @logger.error("Could not clean up instance")
      end
      raise e1
    end

    gen_credential(instance)
  end

  def unprovision(instance_id, credentials_list = [])
    instance = get_instance(instance_id)
    cleanup_instance(instance)
    {}
  end

  def bind(instance_id, binding_options = :all, credentials = nil)
    instance = nil
    if credentials
      instance = get_instance(credentials["name"])
    else
      instance = get_instance(instance_id)
    end
    gen_credential(instance)
  end

  def unbind(credentials)
    {}
  end

  def start_db
    DataMapper.setup(:default, @local_db)
    DataMapper::auto_upgrade!
  end

  def save_instance(instance)
    raise EchoError.new(EchoError::ECHO_SAVE_INSTANCE_FAILED, instance.inspect) unless instance.save
  end

  def destroy_instance(instance)
    raise EchoError.new(EchoError::ECHO_DESTORY_INSTANCE_FAILED, instance.inspect) unless instance.destroy
  end

  def get_instance(name)
    instance = ProvisionedService.get(name)
    raise EchoError.new(EchoError::ECHO_FIND_INSTANCE_FAILED, name) if instance.nil?
    instance
  end

  def cleanup_instance(instance)
    @logger.debug("Cleaning up the instance : #{instance.name}")
    err_msg = []
    begin
      destroy_instance(instance)
    rescue => e
      err_msg << e.message
    end
    raise EchoError.new(EchoError::ECHO_CLEANUP_INSTANCE_FAILED, err_msg.inspect) if err_msg.size > 0
  end

  def gen_credential(instance)
    credential = {
      "host" => get_host,
      "port" => @port,
      "name" => instance.name
    }
  end
end
