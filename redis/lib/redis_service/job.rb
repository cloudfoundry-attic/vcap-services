# Copyright (c) 2009-2011 VMware, Inc.
$LOAD_PATH.unshift File.dirname(__FILE__)

ENV["BUNDLE_GEMFILE"] ||= File.expand_path("../../Gemfile", __FILE__)
require "bundler/setup"
require "vcap_services_base"

module VCAP
  module Services
    module Redis
    end
  end
end

require "job/redis_serialization"
require "job/redis_snapshot"
