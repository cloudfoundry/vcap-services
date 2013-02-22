# Copyright (c) 2009-2011 VMware, Inc.

$LOAD_PATH.unshift File.dirname(__FILE__)

ENV["BUNDLE_GEMFILE"] ||= File.expand_path("../../Gemfile", __FILE__)
require "bundler/setup"
require "vcap_services_base"

module VCAP
  module Services
    module Filesystem
    end
  end
end

require "job/filesystem_serialization"
require "job/filesystem_snapshot"