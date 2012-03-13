# Copyright (c) 2009-2011 VMware, Inc.
$LOAD_PATH.unshift File.dirname(__FILE__)

ENV["BUNDLE_GEMFILE"] ||= File.expand_path("../../Gemfile", __FILE__)
require 'bundler/setup'
require 'vcap_services_base'

include VCAP::Services::Snapshot
module VCAP
  module Services
    module Postgresql
    end
  end
end

module VCAP
  module Services
    module Postgresql
      module Job
        def self.setup_job_logger(logger)
          VCAP::Services::Snapshot.logger = @logger
          VCAP::Services::Serialization.logger = @logger
          VCAP::Services::AsyncJob.logger = @logger
        end
      end
    end
  end
end

require "job/postgresql_serialization"
require "job/postgresql_snapshot"

