# Copyright (c) 2009-2011 VMware, Inc.

require "fileutils"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), ".")
require "filesystem_driver"

module VCAP
  module Services
    module Filesystem
      module Util
        include VCAP::Services::Filesystem::Driver

        def setup_logger(opts={:logger => nil})
          @logger = opts[:logger] unless opts[:logger].nil?
          return @logger if @logger
          @logger = Logger.new( STDOUT)
          @logger.level = Logger::DEBUG
          @logger
        end

        def fmt_error(e)
          "#{e}: [#{e.backtrace.join(" | ")}]"
        end

        # Remove all contents from a filesystem instance
        #  instance: The filesystem ProvisionedService instance
        #
        def remove_filesystem_data(instance)
          path = get_fs_instance_dir(instance)
          FileUtils.rm_rf(path)
        end

        # Create a archive from a filesystem instance
        #  instance: The filesystem ProvisionedService instance
        #  dump_path: Full file path to save the archive to
        #  compressed_file_name: File name for the archive
        #  opts : other_options
        #    tar_bin: path of gzip binary if not in PATH
        #    logger: logger to use
        #
        def dump_filesystem_data(instance, dump_path, compressed_file_name, opts={})
          raise ArgumentError, "Missing options." unless instance && dump_path && compressed_file_name
          setup_logger(opts)

          tar_bin = opts[:tar_bin] || "tar"

          file_path = get_fs_instance_dir(instance)

          dump_file = File.join(dump_path, compressed_file_name)
          cmd = "#{tar_bin} -czf #{dump_file} -C #{file_path} ."
          @logger.info("Taking filesystem snapshot command: #{cmd}")

          FileUtils.mkdir_p(dump_path)
          on_err = Proc.new do |cmd, code, msg|
            raise "CMD '#{cmd}' exit with code: #{code}. Message: #{msg}"
          end
          res = CMDHandle.execute(cmd, nil, on_err)
          res
        rescue => e
          @logger.error("Error dumping #{dump_path} instance to #{dump_file}: #{fmt_error(e)}")
          FileUtils.rm_rf(dump_file) if dump_file
          nil
        end

        # Import an archive from a filesystem instance. This call is destructive removing all previous files.
        #  instance: The filesystem ProvisionedService instance
        #  dump_file: Full file path to the archive to import
        #  opts : other_options
        #    tar_bin: path of gzip binary if not in PATH
        #    logger: logger to use
        #
        def import_filesystem_data(instance, dump_file, opts)
          raise ArgumentError, "Missing options." unless instance && dump_file
          setup_logger(opts)

          tar_bin = opts[:tar_bin] || "tar"

          dump_path = get_fs_instance_dir(instance)
          cmd = "#{tar_bin} -xzf #{dump_file} -C #{dump_path}"
          @logger.info("Importing filesystem snapshot command: #{cmd}")

          remove_filesystem_data(instance)
          FileUtils.mkdir_p(dump_path)
          on_err = Proc.new do |cmd, code, msg|
            raise "CMD '#{cmd}' exit with code: #{code}. Message: #{msg}"
          end
          res = CMDHandle.execute(cmd, nil, on_err)
          res
        rescue => e
          @logger.error("Error dumping instance #{dump_file}: #{fmt_error(e)}")
          nil
        end

      end
    end
  end
end