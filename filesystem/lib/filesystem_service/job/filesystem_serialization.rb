# Copyright (c) 2009-2011 VMware, Inc.

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..')
require "util"
require "filesystem_error"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..')
require "filesystem_service/filesystem_node"

module VCAP::Services::Filesystem::Serialization
  include VCAP::Services::Base::AsyncJob::Serialization

  # Validate the serialized data file.
  def validate_input(files, manifest)
    raise "Doesn't contains any snapshot file." if files.empty?
    raise "Invalid version:#{version}" if manifest[:version] != 1
    true
  end

  class ImportFromURLJob < BaseImportFromURLJob
    include VCAP::Services::Filesystem::Serialization
  end
end