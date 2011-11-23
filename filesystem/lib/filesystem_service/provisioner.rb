# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), ".")

require "base/provisioner"
require "filesystem_service/common"
require "filesystem_service/error"
require "uuidtools"

class VCAP::Services::Filesystem::Provisioner < VCAP::Services::Base::Provisioner

  include VCAP::Services::Filesystem::Common
  include VCAP::Services::Filesystem

  FILESYSTEM_CONFIG_FILE = File.expand_path("../../../config/filesystem_gateway.yml", __FILE__)

  def initialize(options)
    super(options)
    @backends = options[:additional_options][:backends] || get_filesystem_config
    @backend_index = rand(@backends.size)
    @logger.debug("backends: #{@backends.inspect}")
  end

  def provision_service(request, prov_handle=nil, &blk)
    @logger.debug("[#{service_description}] Attempting to provision instance (request=#{request.extract})")
    name = UUIDTools::UUID.random_create.to_s
    backend = get_backend
    raise FilesystemError.new(FilesystemError::FILESYSTEM_GET_BACKEND_FAILED) if backend == nil
    instance_dir = get_instance_dir(name, backend)
    begin
      FileUtils.mkdir(instance_dir)
    rescue => e
      raise FilesystemError.new(FilesystemError::FILESYSTEM_CREATE_INSTANCE_DIR_FAILED, instance_dir)
    end
    begin
      FileUtils.chmod(0777, instance_dir)
    rescue => e
      raise FilesystemError.new(FilesystemError::FILESYSTEM_CHANGE_INSTANCE_DIR_PERMISSION_FAILED, instance_dir)
    end
    prov_req = ProvisionRequest.new
    prov_req.plan = request.plan
    # use old credentials to provision a service if provided.
    prov_req.credentials = prov_handle["credentials"] if prov_handle

    credentials = gen_credentials(name, backend)
    svc = {
      :data => prov_req.dup,
      :service_id => name,
      :credentials => credentials
    }
    # FIXME: workaround for inconsistant representation of bind handle and provision handle
    svc_local = {
      :configuration => prov_req.dup,
      :service_id => name,
      :credentials => credentials
    }
    @logger.debug("Provisioned #{svc.inspect}")
    @prov_svcs[svc[:service_id]] = svc_local
    blk.call(success(svc))
  rescue => e
    if e.instance_of? FilesystemError
      blk.call(failure(e))
    else
      @logger.warn(e)
      blk.call(internal_fail)
    end
  end

  def unprovision_service(instance_id, &blk)
    @logger.debug("[#{service_description}] Attempting to unprovision instance (instance id=#{instance_id}")
    svc = @prov_svcs[instance_id]
    raise FilesystemError.new(FilesystemError::FILESYSTEM_FIND_INSTANCE_FAILED, instance_id) if svc == nil
    host = svc[:credentials]["internal"]["host"]
    export = svc[:credentials]["internal"]["export"]
    backend = get_backend(host, export)
    raise FilesystemError.new(FilesystemError::FILESYSTEM_GET_BACKEND_BY_HOST_AND_EXPORT_FAILED, host, export) if backend == nil
    FileUtils.rm_rf(get_instance_dir(instance_id, backend))
    bindings = find_all_bindings(instance_id)
    bindings.each do |b|
      @prov_svcs.delete(b[:service_id])
    end
    blk.call(success())
  rescue => e
    if e.instance_of? FilesystemError
      blk.call(failure(e))
    else
      @logger.warn(e)
      blk.call(internal_fail)
    end
  end

  def bind_instance(instance_id, binding_options, bind_handle=nil, &blk)
    @logger.debug("[#{service_description}] Attempting to bind to service #{instance_id}")
    svc = @prov_svcs[instance_id]
    raise FilesystemError.new(FilesystemError::FILESYSTEM_FIND_INSTANCE_FAILED, instance_id) if svc == nil

    #FIXME options = {} currently, should parse it in future.
    request = BindRequest.new
    request.name = instance_id
    request.bind_opts = binding_options
    service_id = nil
    if bind_handle
      request.credentials = bind_handle["credentials"]
      service_id = bind_handle["service_id"]
    else
      service_id = UUIDTools::UUID.random_create.to_s
    end

    # Save binding-options in :data section of configuration
    config = svc[:configuration].clone
    config['data'] ||= {}
    config['data']['binding_options'] = binding_options
    res = {
      :service_id => service_id,
      :configuration => config,
      :credentials => svc[:credentials]
    }
    @logger.debug("[#{service_description}] Binded: #{res.inspect}")
    @prov_svcs[res[:service_id]] = res
    blk.call(success(res))
  rescue => e
    if e.instance_of? FilesystemError
      blk.call(failure(e))
    else
      @logger.warn(e)
      blk.call(internal_fail)
    end
  end

  def unbind_instance(instance_id, handle_id, binding_options, &blk)
    @logger.debug("[#{service_description}] Attempting to unbind to service #{instance_id}")
    blk.call(success())
  end

  def get_filesystem_config
    config_file = YAML.load_file(FILESYSTEM_CONFIG_FILE)
    config = VCAP.symbolize_keys(config_file)
    config[:backends]
  end

  def get_backend(host=nil, export=nil)
    if host && export
      @backends.each do |backend|
        if backend["host"] == host && backend["export"] == export
          return backend
        end
      end
      return nil
    else
      return nil if @backends == nil
      index = @backend_index
      @backend_index = (@backend_index + 1) % @backends.size
      return @backends[index]
    end
  end

  def get_instance_dir(name, backend)
    File.join(backend["mount"], name)
  end

  def gen_credentials(name, backend)
    credentials = {
      "internal" => {
        "name" => name,
        "host" => backend["host"],
        "export" => backend["export"],
      }
    }
  end

end
