# Copyright (c) 2009-2012 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), ".")

require 'fileutils'
require 'redis'
require "uuidtools"

module VCAP
  module Services
    module Filesystem
      class Node < VCAP::Services::Base::Node
      end
    end
  end
end

require "filesystem_service/common"
require "filesystem_service/filesystem_driver"
require "filesystem_service/filesystem_error"
require "filesystem_service/util"

class VCAP::Services::Filesystem::Node
  include VCAP::Services::Filesystem::Common
  include VCAP::Services::Filesystem::Driver
  include VCAP::Services::Filesystem::Util
  include VCAP::Services::Filesystem

  class ProvisionedService
    include DataMapper::Resource

    property :name,       String,   :key => true
    property :fs_type,    String,   :required => true
    property :backend,    Json
    property :pid,        Integer
  end

  def initialize(options)
    super(options)

    @supported_versions = options[:supported_versions]
    @default_version = options[:default_version]

    @redis_port       = options[:redis]["port"]
    @redis_ip         = options[:redis]["ip"]
    @redis_passwd     = options[:redis]["passwd"]
    @wake_interval    = options[:wake_interval]
    @local_db         = options[:local_db]
    @req_id           = "fss_req:#{@node_id}".freeze
    @usage_id         = "fss_usage:#{@node_id}".freeze
    @backends         = options[:backends]
    @backend_index    = rand(@backends.size)
    @fs_type          = options[:fs_type]
    @quota            = options[:quota] || 1024
    @tar_bin         = options[:tar_bin]
    @logger.debug("backends: #{@backends.inspect}")

    @base_dir = options[:base_dir]
    FileUtils.mkdir_p(@base_dir)
  end

  def pre_send_announcement
    DataMapper.setup(:default, @local_db)
    DataMapper::auto_upgrade!

    @capacity_lock.synchronize do
      @capacity -= ProvisionedService.all.size
    end
  end

  def announcement
    @capacity_lock.synchronize do
      { :available_capacity => @capacity,
        :capacity_unit => capacity_unit }
    end
  end

  def get_version(provisioned_service)
    if provisioned_service.version.to_s.empty?
      @default_version
    else
      provisioned_service.version
    end
  end

  def provision(plan, credential=nil, version=nil)
    raise FilesystemError.new(FilesystemError::FILESYSTEM_INVALID_PLAN, plan) unless plan.to_s == @plan
    instance = ProvisionedService.new
    if credential
      name    = credential["name"]
      backend = get_backend(credential)
    else
      name = UUIDTools::UUID.random_create.to_s
      backend = get_backend
    end
    raise FilesystemError.new(FilesystemError::FILESYSTEM_GET_BACKEND_FAILED) if backend == nil
    instance.name     = name
    instance.fs_type  = @fs_type
    instance.backend  = backend
    instance_dir      = get_fs_instance_dir(instance)

    begin
      FileUtils.mkdir(instance_dir)
    rescue => e
      raise FilesystemError.new(FilesystemError::FILESYSTEM_CREATE_INSTANCE_DIR_FAILED, instance_dir)
    end
    begin
      FileUtils.chmod(0777, instance_dir)
    rescue => e
      cleanup_instance(instance)
      raise FilesystemError.new(FilesystemError::FILESYSTEM_CHANGE_INSTANCE_DIR_PERMISSION_FAILED, instance_dir)
    end

    begin
      raise unless instance.save
    rescue => e
      @logger.error("Could not save entry: #{instance.errors.inspect}")
      cleanup_instance(instance)
      raise FilesystemError.new(FilesystemError::FILESYSTEM_SAVE_INSTANCE_FAILED, instance.inspect)
    end
    gen_credentials(instance.name, instance.backend)
  end

  def unprovision(name, credentials=[])
    instance = get_instance(name)
    cleanup_instance(instance)
    {}
  end

  def bind(name, bind_opts={}, credential=nil)
    instance = nil
    if credential
      instance = get_instance(credential["name"])
    else
      instance = get_instance(name)
    end
    gen_credentials(instance.name, instance.backend)
  end

  def unbind(credential)
    {}
  end

  def get_instance(name)
    instance = ProvisionedService.get(name)
    raise FilesystemError.new(FilesystemError::FILESYSTEM_FIND_INSTANCE_FAILED, name) if instance.nil?
    instance
  end

  def cleanup_instance(instance)
    remove_filesystem_data(instance)
    raise FilesystemError.new(FilesystemError::FILESYSTEM_CLEANUP_INSTANCE_FAILED, instance.inspect) unless instance.new? || instance.destroy
  end

  def gen_credentials(name, backend)
    credentials = {
        "fs_type"     => @fs_type,
        "name"        => name,
        "internal"    => {
            "node_id" => @node_id,
            "quota"   => @quota,
        }.merge(gen_fs_credentials(@fs_type, backend))
    }
  end

  def get_backend(cred=nil)
    if cred
      return get_fs_backend(cred, @fs_type, @backends)
    else
      # Simple round-robin load-balancing; TODO: Something smarter?
      return nil if @backends == nil || @backends.empty?
      index = @backend_index
      @backend_index = (@backend_index + 1) % @backends.size
      return @backends[index]
    end
  end

  def get_instance_dir(instance)
    get_fs_instance_dir(instance)
  end

end
