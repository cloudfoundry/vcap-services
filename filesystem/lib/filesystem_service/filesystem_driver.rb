# Copyright (c) 2009-2011 VMware, Inc.

require "fileutils"

module VCAP
  module Services
    module Filesystem
      module Driver

        def get_fs_instance_dir(instance)
          return File.join(instance.backend["mount"], instance.name)
        end

        def get_fs_backend(cred, fs_type, backends)
          case fs_type
            when /nfs/i
              host    = cred["internal"]["host"]
              export  = cred["internal"]["export"]
              backends.each do |backend|
                if backend["host"] == host && backend["export"] == export
                  return backend
                end
              end if host && export
            else
              # Local filesystem
              mount_path = cred["internal"]["mount"]
              backends.each do |backend|
                if backend["mount"] == mount_path
                  return backend
                end
              end if mount_path
          end
          return nil
        end

        def gen_fs_credentials(fs_type, backend)
          credentials = {}

          case fs_type
            when /nfs/i
              credentials["host"] = backend["host"]
              credentials["export"] = backend["export"]
          end

          credentials["mount"] = backend["mount"]
          credentials
        end

      end
    end
  end
end