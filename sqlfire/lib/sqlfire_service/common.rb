# Copyright (c) 2009-2011 VMware, Inc.

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "..", "..", "base", "lib")
$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "..", "..", "base", "lib", "base")
require 'base/service_message'

module VCAP
  module Services
    module Sqlfire
      module Common
        def service_name
          "SqlfireaaS"
        end
      end
    end
  end
end

# Extend the ProvisionRequest class
module VCAP
  module Services
    module Internal
      class ProvisionRequest
        required :name, String
        optional :options, Hash
      end
    end
  end
end
