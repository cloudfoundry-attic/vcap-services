# Copyright (c) 2009-2011 VMware, Inc.
$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..', 'base', 'lib')
require 'base/service_error'

module VCAP
  module Services
    module Redis
      class RedisError < VCAP::Services::Base::Error::ServiceError
        # 31100 - 31199  Redis-specific Error
        REDIS_SAVE_INSTANCE_FAILED        = [31100, HTTP_INTERNAL, "Could not save instance: %s"]
        REDIS_DESTORY_INSTANCE_FAILED     = [31101, HTTP_INTERNAL, "Could not destroy instance: %s"]
        REDIS_FIND_INSTANCE_FAILED        = [31102, HTTP_NOT_FOUND, "Could not find instance: %s"]
        REDIS_START_INSTANCE_FAILED       = [31103, HTTP_INTERNAL, "Could not start instance: %s"]
        REDIS_STOP_INSTANCE_FAILED        = [31104, HTTP_INTERNAL, "Could not stop instance: %s"]
        REDIS_INVALID_PLAN                = [31105, HTTP_INTERNAL, "Invalid plan: %s"]
        REDIS_CLEANUP_INSTANCE_FAILED     = [31106, HTTP_INTERNAL, "Could not cleanup instance, the reasons: %s"]
        REDIS_CONNECT_INSTANCE_FAILED     = [31107, HTTP_INTERNAL, "Could not connect redis instance"]
        REDIS_SET_INSTANCE_PASS_FAILED    = [31108, HTTP_INTERNAL, "Could not set redis instance password"]
      end
    end
  end
end
