# Copyright (c) 2012 NTT Communications
#       UEMURA Yuichi <y.uemura@ntt.com>
#       OZAWA Tsuyoshi <ozawa.tsuyoshi@gmail.com>
# Copyright (c) 2009-2011 VMware, Inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
# This code is based on Redis as a Service.

$:.unshift File.join(File.dirname(__FILE__), '..')
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

ENV["BUNDLE_GEMFILE"] ||= File.expand_path("../../Gemfile", __FILE__)

require 'rubygems'
require 'rspec'
require 'bundler/setup'
require 'nats/client'
require 'vcap/common'
require "datamapper"
require "uri"
require "thread"


def get_hostname(credentials)
  host = credentials['host']
  port = credentials['port'].to_s
  hostname = host + ":" + port
  return hostname
end

def get_connect_info(credentials)
  ret = []
  hostname = get_hostname(credentials)
  username = @credentials['user']
  password = @credentials['password']

  return [hostname, username, password]
end
