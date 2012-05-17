# Copyright (c) 2009-2011 VMware, Inc.
$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..', 'base', 'lib')
require 'base/service_error'

module VCAP
  module Services
    module Echo
      class EchoError < VCAP::Services::Base::Error::ServiceError
        # 31100 - 31199  Echo-specific Error
        ECHO_SAVE_INSTANCE_FAILED        = [31100, HTTP_INTERNAL, "Could not save instance: %s"]
        ECHO_DESTORY_INSTANCE_FAILED     = [31101, HTTP_INTERNAL, "Could not destroy instance: %s"]
        ECHO_FIND_INSTANCE_FAILED        = [31102, HTTP_NOT_FOUND, "Could not find instance: %s"]
        ECHO_START_INSTANCE_FAILED       = [31103, HTTP_INTERNAL, "Could not start instance: %s"]
        ECHO_STOP_INSTANCE_FAILED        = [31104, HTTP_INTERNAL, "Could not stop instance: %s"]
        ECHO_INVALID_PLAN                = [31105, HTTP_INTERNAL, "Invalid plan: %s"]
        ECHO_CLEANUP_INSTANCE_FAILED     = [31106, HTTP_INTERNAL, "Could not cleanup instance, the reasons: %s"]
      end
    end
  end
end
