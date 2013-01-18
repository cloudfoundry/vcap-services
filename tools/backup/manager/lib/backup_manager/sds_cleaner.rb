# Copyright (c) 2009-2013 VMware, Inc.
require 'time'
require 'em-http'
require 'json'
require 'json_message'
require 'services/api'
require 'fiber'

module VCAP
  module Services
    module Backup
    end
  end
end

require 'util'

class VCAP::Services::Backup::SdsCleaner
  include VCAP::Services::Backup::LifecycleUtils
end
