# Copyright (c) 2009-2011 VMware, Inc.
require 'postgresql_service/pg_timeout'

module VCAP
  module Services
    module Postgresql
    end
   end
end

# Give various helper functions on version differences
module VCAP::Services::Postgresql::Version

  def pg_version(conn, opts={})
    # by default, will return major.minor
    # if :full option set, will return major.minor.revision
    # if :major option set, will return major
    opts[:full] ||= false
    opts[:full] ? opts[:major] = false : opts[:major] ||= false

    begin
      # not to use the select version() for it will provide build information
      version_str = conn.parameter_status('server_version')
      # version format: major.minor.revision
      reg = /\b(\d+)\.(\d+)\.([^\s]+)\b/
      version_info = version_str.scan(reg)[0]
      if version_info && version_info.kind_of?(Array) && version_info.size == 3
        return opts[:full] ? version_info.join(".") : ( opts[:major] ? version_info[0] : version_info[0..1].join('.'))
      else
        raise "Unsupported postgresql version format"
      end
    rescue => e
      raise "Fail to retrieve version from connection for #{e}"
    end
  end

  def pg_stat_activity_pid_field(version)
    case version
    when '9.2'
      'pid'
    else
      'procpid'
    end
  end

  def pg_stat_activity_query_field(version)
    case version
    when '9.2'
      'query'
    else
      'current_query'
    end
  end
end
