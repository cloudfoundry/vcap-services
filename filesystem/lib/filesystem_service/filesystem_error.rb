# Copyright (c) 2009-2012 VMware, Inc.

module VCAP
  module Services
    module Filesystem
      class FilesystemError < VCAP::Services::Base::Error::ServiceError
        # 31900 - 31999  Filesystem-specific Error
        FILESYSTEM_CREATE_INSTANCE_DIR_FAILED             = [31900, HTTP_INTERNAL, "Could not create instance directory: %s"]
        FILESYSTEM_CHANGE_INSTANCE_DIR_PERMISSION_FAILED  = [31901, HTTP_INTERNAL, "Could not change access permission of instance directory: %s"]
        FILESYSTEM_GET_BACKEND_FAILED                     = [31902, HTTP_INTERNAL, "Could not get backend"]
        FILESYSTEM_GET_BACKEND_BY_HOST_AND_EXPORT_FAILED  = [31903, HTTP_INTERNAL, "Could not get backend by host %s and export %s"]
        FILESYSTEM_FIND_INSTANCE_FAILED                   = [31904, HTTP_NOT_FOUND, "Could not find instance: %s"]
        FILESYSTEM_INVALID_PLAN                           = [31905, HTTP_INTERNAL, "Invalid plan: %s"]
        FILESYSTEM_CLEANUP_INSTANCE_FAILED                = [31906, HTTP_INTERNAL, "Could not cleanup instance, the reasons: %s"]
        FILESYSTEM_SAVE_INSTANCE_FAILED                   = [31907, HTTP_INTERNAL, "Could not save instance: %s"]
      end
    end
  end
end
