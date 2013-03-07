# Copyright (c) 2009-2011 VMware, Inc.
require 'pg'
require 'tempfile'
require 'fileutils'
require 'open3'
require 'postgresql_service/pg_timeout'

module VCAP
  module Services
    module Postgresql
    end
   end
end

# Give various helper functions on version differences
module VCAP::Services::Postgresql::Version
  # Return the version of postgresql
  def pg_version(conn)
    return '-1' unless conn
    version = conn.query("select version()")
    reg = /([0-9.]{5})/
    return version[0]['version'].scan(reg)[0][0]
  end

  def pg_stat_activity_pid_field(version)
    case version
    when /^9\.2/
      'pid'
    else
      'procpid'
    end
  end

  def pg_stat_activity_query_field(version)
    case version
    when /^9\.2/
      'query'
    else
      'current_query'
    end
  end
end
