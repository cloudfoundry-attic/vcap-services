# Copyright (c) 2009-2012 VMware, Inc.
require "redis"
require "resque"

module VCAP
  module Services
    module Base
      module AsyncJob
      end
    end
  end
end

class VCAP::Services::Base::AsyncJob::Config
  class << self
    attr_reader :redis_config, :redis, :logger
    def redis_config=(config)
      @redis_config = config
      @redis = ::Redis.new config
      Resque.redis = @redis
    end

    def logger=(logger)
      @logger = logger
    end
  end
end
