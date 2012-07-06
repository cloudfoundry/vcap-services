# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), '.')

require_relative "common"
require "uuidtools"
require_relative "appdirect_helper"
require_relative "appdirect_error"

class VCAP::Services::AppDirect::Provisioner < VCAP::Services::Base::Provisioner

  include VCAP::Services::AppDirect::Common
  include VCAP::Services::AppDirect

  ADGW_CONFIG_FILE = File.expand_path("../../../config/apdirect_gateway.yml", __FILE__)

  def to_s
    "VCAP::Services::AppDirect::Provisioner instance: #{@apdirect_config.inspect}"
  end

  def get_appdirect_config
    config_file = YAML.load_file(ADGW_CONFIG_FILE)
    config = VCAP.symbolize_keys(config_file)
    config[:appdirect]
  end

  def initialize(options)
    super(options)
    @apdirect_config = options[:additional_options][:apdirect] || get_appdirect_config
    @logger.debug "apdirect_config: #{@apdirect_config.inspect}"

    @host = @apdirect_config[:host]
    @port = @apdirect_config[:port]

    @apdirect_helper = VCAP::Services::AppDirect::Helper.new(@apdirect_config, @logger)
  end

  def provision_service(request, prov_handle=nil, &blk)
    @logger.debug("[#{service_description}] Attempting to provision instance (request=#{request.extract})")
    begin
      st_name = UUIDTools::UUID.random_create.to_s
      st_id = @apdirect_helper.create_subtenant(st_name)

      # should we create subtenant admin rather than uid here?
      token = UUIDTools::UUID.random_create.to_s
      shared_secret = @apdirect_helper.create_user(token, st_name)

      svc = {
        :data => {:subtenant_name => st_name, :subtenant_id => st_id, :host => @host},
        :service_id => st_name,
        :credentials => {:host => @host, :port => @port, :token => token,
          :shared_secret => shared_secret, :subtenant_id => st_id}
      }
      # set 'configuration' instead of 'data' to keep local hash consistent
      svc_local = {
        :configuration => {"subtenant_name" => st_name, "subtenant_id" => st_id, "host" => @host},
        :service_id => st_name,
        :credentials => {"host" => @host, "port" => @port, "token" => token,
          "shared_secret" => shared_secret, "subtenant_id" => st_id}
      }
      @logger.debug("Service provisioned: #{svc.inspect}")
      @prov_svcs[svc[:service_id]] = svc_local
      blk.call(success(svc))
    rescue => e
      # roll back work
      @logger.warn 'provision error, trying to roll back if necessary'
      begin
        @apdirect_helper.delete_subtenant(st_name) if st_id
      rescue => e1
        @logger.info 'roll back error'
      end
      if e.instance_of? AppDirectError
        blk.call(failure(e))
      else
        @logger.warn(e)
        blk.call(internal_fail)
      end
    end
  end

  def unprovision_service(instance_id, &blk)
    @logger.debug("[#{service_description}] Attempting to unprovision instance (instance id=#{instance_id}")
    begin
      success = @apdirect_helper.delete_subtenant(instance_id)
      if success
        @logger.debug("service unprovisioned: #{instance_id} ")
        # clean up local hash
        remove_local_bindings(instance_id)
        @prov_svcs.delete(instance_id)
      end
      blk.call(success())
    rescue => e
      if e.instance_of? AppDirectError
        blk.call(failure(e))
      else
        @logger.warn(e)
        blk.call(internal_fail)
      end
    end
  end

  def bind_instance(instance_id, binding_options, bind_handle=nil, &blk)
    @logger.debug("attempting to bind service: #{instance_id}")
    if instance_id.nil?
      @logger.warn("#{instance_id} is null!")
      blk.call(internal_fail)
    end

    begin
      svc = @prov_svcs[instance_id]
      raise "#{instance_id} not found!" if svc.nil?
      @logger.debug("svc[configuration]: #{svc[:configuration]}")

      token = UUIDTools::UUID.random_create.to_s
      shared_secret = @apdirect_helper.create_user(token, instance_id)

      res = {
        :service_id => token,
        :configuration => svc[:configuration],
        :credentials => {:host => @host, :port => @port, :token => token,
          :shared_secret => shared_secret, :subtenant_id => svc[:configuration]["subtenant_id"]}
      }
      res_local = {
        :service_id => token,
        :configuration => svc[:configuration],
        :credentials => {'host' => @host, 'port' => @port, 'token' => token,
          'shared_secret' => shared_secret, 'subtenant_id' => svc[:configuration]["subtenant_id"]}
      }
      @logger.debug("binded: #{res.inspect}")
      @prov_svcs[res[:service_id]] = res_local
      blk.call(success(res))
    rescue => e
      if e.instance_of? AppDirectError
        blk.call(failure(e))
      else
        @logger.warn(e)
        blk.call(internal_fail)
      end
    end
  end

  def unbind_instance(instance_id, handle_id, binding_options, &blk)
    @logger.debug("attempting to unbind service: #{instance_id}")
    begin
      raise "instance_id cannot be nil" if instance_id.nil?
      svc = @prov_svcs[handle_id]
      raise "#{handle_id} not found!" if svc.nil?

      @logger.debug("svc[configuration]: #{svc[:configuration]}")
      success = @apdirect_helper.delete_user(handle_id, instance_id)
      @prov_svcs.delete(handle_id) if success
      blk.call(success())
    rescue => e
      if e.instance_of? AppDirectError
        blk.call(failure(e))
      else
        @logger.warn(e)
        blk.call(internal_fail)
      end
    end
  end

  def remove_local_bindings(subtenant_name)
    @logger.debug "remove_local_bindings with subtenant_name: #{subtenant_name}"

    # get subtenant_id by subtenant_name
    subtenant_id = nil
    @prov_svcs.each do |k, v|
      if v[:service_id] == subtenant_name
        subtenant_id = v[:credentials]['subtenant_id']
        @logger.debug "right subtenant_id found: #{subtenant_id}"
        break
      end
    end

    # remove related bindings from local hash
    if subtenant_id
      @prov_svcs.each do |k,v|
        if v[:credentials]['subtenant_id'] == subtenant_id && v[:service_id] != subtenant_name
          @logger.debug "delete binding with token: #{k}"
          @prov_svcs.delete(k)
        end
      end
    end
  end
end
