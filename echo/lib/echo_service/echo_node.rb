# Copyright (c) 2009-2011 VMware, Inc.
require "fileutils"
require "logger"
require "datamapper"
require "uuidtools"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..', 'base', 'lib')
require 'base/node'

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
    property :plan,       Enum[:free], :required => true
    property :memory,     Integer
  end

  def initialize(options)
    super(options)

    @base_dir = options[:base_dir]
    FileUtils.mkdir_p(@base_dir)
    @local_db = options[:local_db]
    @host = options[:host]
    @port = options[:port]
  end

  def pre_send_announcement
    super
    start_db
    ProvisionedService.all.each do |instance|
      @capacity -= capacity_unit 
    end
  end

  def announcement
    a = { :available_capacity => @capacity,
			:capacity_unit => capacity_unit }
  end

  def provision(plan, credentials = nil)
    instance = ProvisionedService.new
    instance.plan = plan
    if credentials
      instance.name = credentials["name"]
    else
      instance.name = UUIDTools::UUID.random_create.to_s
    end

    begin
      save_instance(instance)
    rescue => e1
      begin
        cleanup_instance(instance)
      rescue => e2
        # Ignore the rollback exception
      end
      raise e1
    end

    gen_credentials(instance)
  end

  def unprovision(instance_id, credentials_list = [])
    instance = get_instance(instance_id)
    cleanup_instance(instance)
    {}
  end

  def bind(instance_id, binding_options = :all, credentials = nil)
    # FIXME: Echo has no user level security, just return provisioned credentials.
    instance = nil
    if credentials
      instance = get_instance(credentials["name"])
    else
      instance = get_instance(instance_id)
    end
    gen_credentials(instance)
  end

  def unbind(credentials)
    # FIXME: Echo has no user level security, so has no operation for unbinding.
    {}
  end

  def start_db
    DataMapper.setup(:default, @local_db)
    DataMapper::auto_upgrade!
  end

  def save_instance(instance)
    raise EchoError.new(EchoError::REDIS_SAVE_INSTANCE_FAILED, instance.inspect) unless instance.save
  end

  def destroy_instance(instance)
    raise EchoError.new(EchoError::REDIS_DESTORY_INSTANCE_FAILED, instance.inspect) unless instance.destroy
  end

  def get_instance(name)
    instance = ProvisionedService.get(name)
    raise EchoError.new(EchoError::REDIS_FIND_INSTANCE_FAILED, name) if instance.nil?
    instance
  end

  def cleanup_instance(instance)
    err_msg = []
    begin
      destroy_instance(instance)
    rescue => e
      err_msg << e.message
    end
    raise EchoError.new(EchoError::REDIS_CLEANUP_INSTANCE_FAILED, err_msg.inspect) if err_msg.size > 0
  end

  def memory_for_instance(instance)
    case instance.plan
      when :free then 16
      else
        raise EchoError.new(EchoError::REDIS_INVALID_PLAN, instance.plan)
    end
  end

  def gen_credentials(instance)
    credentials = {
      "hostname" => @local_ip,
      "host" => @host,
      "port" => @port,
      "name" => instance.name
    }
  end

end
