# Copyright (c) 2009-2011 VMware, Inc.

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..")
require "util"
require "filesystem_error"

module VCAP::Services::Filesystem::Snapshot
  include VCAP::Services::Base::AsyncJob::Snapshot

  def init_localdb(database_url)
    DataMapper.setup(:default, database_url)
  end

  def filesystem_provisioned_service
    VCAP::Services::Filesystem::Node::ProvisionedService
  end

  # Dump a filesystem into an archive and save the snapshot information into redis.
  class CreateSnapshotJob < BaseCreateSnapshotJob
    include VCAP::Services::Filesystem::Snapshot
    include VCAP::Services::Filesystem::Util

    def execute
      init_localdb(@config["local_db"])
      opts = { "tar_bin" => @config["tar_bin"], :logger => @logger }

      instance = filesystem_provisioned_service.get(name)
      dump_path = get_dump_path(name, snapshot_id)
      archive_name = "#{snapshot_id}.tgz"
      dump_file = File.join(dump_path, archive_name)

      result = dump_filesystem_data(instance, dump_path, archive_name, opts)
      raise "Failed to execute dump command to #{dump_file}" unless result

      dump_file_size = -1
      File.open(dump_file) {|f| dump_file_size = f.size}
      complete_time = Time.now
      snapshot = {
          :snapshot_id => snapshot_id,
          :size => dump_file_size,
          :files => [archive_name],
          :manifest => {
              :version => 1
          }
      }

      snapshot
    end
  end

  # Rollback data from snapshot files.
  class RollbackSnapshotJob < BaseRollbackSnapshotJob
    include VCAP::Services::Filesystem::Snapshot
    include VCAP::Services::Filesystem::Util

    def execute
      init_localdb(@config["local_db"])
      opts = { "tar_bin" => @config["tar_bin"], :logger => @logger }

      instance = filesystem_provisioned_service.get(name)

      dump_file_name = @snapshot_files[0]
      raise "Can't find snapshot file #{snapshot_file_path}" unless File.exists?(dump_file_name)

      result = import_filesystem_data(instance, dump_file_name, opts)
      raise "Failed execute import command to #{name}" unless result
      instance.pid = result
      instance.save

      true
    end
  end

end