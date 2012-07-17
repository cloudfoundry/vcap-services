# Copyright (c) 2009-2012 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), ".")

require "filesystem_service/common"

class VCAP::Services::Filesystem::Provisioner < VCAP::Services::Base::Provisioner
  include VCAP::Services::Filesystem::Common
end
