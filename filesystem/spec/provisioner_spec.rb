# Copyright (c) 2009-2012 VMware, Inc.
require File.dirname(__FILE__) + '/spec_helper'

require 'filesystem_service/provisioner'

describe VCAP::Services::Filesystem::Provisioner do
  it "should be fine to be initialized with default config" do
    EM.run do
      config_file = File.join(config_base_dir, "filesystem_gateway.yml")
      config = YAML.load_file(config_file)
      config = config.inject({}){ |m, (k, v)| m[k.to_sym] = v; m }
      config[:logger] = Logger.new('/dev/null')
      expect { @provisioner = VCAP::Services::Filesystem::Provisioner.new(config) }.to_not raise_error
      EM.add_timer(1) { EM.stop }
    end
  end
end
