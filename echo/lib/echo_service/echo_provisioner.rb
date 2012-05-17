# Copyright (c) 2009-2011 VMware, Inc.
$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..', 'base', 'lib')
require 'base/provisioner'
require 'echo_service/common'

class VCAP::Services::Echo::Provisioner < VCAP::Services::Base::Provisioner

  include VCAP::Services::Echo::Common

  def node_score(node)
    node['available_capacity']
  end

end
