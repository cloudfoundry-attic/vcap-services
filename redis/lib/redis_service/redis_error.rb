# Copyright (c) 2009-2011 VMware, Inc.
$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..', 'base', 'lib')
require 'base/service_error'

module VCAP
  module Services
    module Redis
      class RedisError < VCAP::Services::Base::Error::ServiceError
        # 31100 - 31199  Redis-specific Error
        REDIS_SAVE_SERVICE_FAILED    = [31100, HTTP_INTERNAL, "Could not save service: %s"]
        REDIS_DESTORY_SERVICE_FAILED = [31101, HTTP_INTERNAL, "Could not destroy service: %s"]
        REDIS_FIND_SERVICE_FAILED    = [31102, HTTP_NOT_FOUND, "Could not find service: %s"]
        REDIS_START_SERVICE_FAILED   = [31103, HTTP_INTERNAL, "Could not start service: %s"]
        REDIS_STOP_SERVICE_FAILED    = [31104, HTTP_INTERNAL, "Could not stop service: %s"]
        REDIS_INVALID_PLAN           = [31105, HTTP_INTERNAL, "Invalid plan: %s"]
      end
    end
  end
end
