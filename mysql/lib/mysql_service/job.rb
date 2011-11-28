# Copyright (c) 2009-2011 VMware, Inc.
$LOAD_PATH.unshift File.dirname(__FILE__)

module VCAP
  module Services
    module Mysql
    end
  end
end

require "resque/job_with_status"
require "job/async_job"
require "job/serialization"
require "job/snapshot"
