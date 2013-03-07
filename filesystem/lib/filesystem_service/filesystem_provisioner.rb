# Copyright (c) 2009-2012 VMware, Inc.

require 'fileutils'
require 'redis'
require 'base64'

$:.unshift File.join(File.dirname(__FILE__), ".")

require "filesystem_service/common"
require "filesystem_service/job"

class VCAP::Services::Filesystem::Provisioner < VCAP::Services::Base::Provisioner
  include VCAP::Services::Filesystem::Common

  def create_snapshot_job
    VCAP::Services::Filesystem::Snapshot::CreateSnapshotJob
  end

  def rollback_snapshot_job
    VCAP::Services::Filesystem::Snapshot::RollbackSnapshotJob
  end

  def delete_snapshot_job
    VCAP::Services::Base::AsyncJob::Snapshot::BaseDeleteSnapshotJob
  end

  def create_serialized_url_job
    VCAP::Services::Base::AsyncJob::Serialization::BaseCreateSerializedURLJob
  end

  def import_from_url_job
    VCAP::Services::Filesystem::Serialization::ImportFromURLJob
  end

end
