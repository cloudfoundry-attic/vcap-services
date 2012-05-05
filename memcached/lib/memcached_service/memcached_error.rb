# Copyright (c) 2009-2011 VMware, Inc.
# This code is based on Redis as a Service.

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..', 'base', 'lib')
require 'base/service_error'

module VCAP
  module Services
    module Memcached
      class MemcachedError < VCAP::Services::Base::Error::ServiceError
        # 31100 - 31199  Memcached-specific Error
        # FIXME: This error code is paste from redis.
        MEMCACHED_SAVE_INSTANCE_FAILED        = [31100, HTTP_INTERNAL, "Could not save instance: %s"]
        MEMCACHED_DESTROY_INSTANCE_FAILED     = [31101, HTTP_INTERNAL, "Could not destroy instance: %s"]
        MEMCACHED_FIND_INSTANCE_FAILED        = [31102, HTTP_NOT_FOUND, "Could not find instance: %s"]
        MEMCACHED_START_INSTANCE_FAILED       = [31103, HTTP_INTERNAL, "Could not start instance: %s"]
        MEMCACHED_STOP_INSTANCE_FAILED        = [31104, HTTP_INTERNAL, "Could not stop instance: %s"]
        MEMCACHED_INVALID_PLAN                = [31105, HTTP_INTERNAL, "Invalid plan: %s"]
        MEMCACHED_CLEANUP_INSTANCE_FAILED     = [31106, HTTP_INTERNAL, "Could not cleanup instance, the reasons: %s"]
        MEMCACHED_CONNECT_INSTANCE_FAILED     = [31107, HTTP_INTERNAL, "Could not connect memcached instance"]
        MEMCACHED_SET_INSTANCE_PASS_FAILED    = [31108, HTTP_INTERNAL, "Could not set memcached instance password"]
        MEMCACHED_RESTORE_FILE_NOT_FOUND      = [31109, HTTP_INTERNAL, "Could not find memcached restore data file %s"]
        MEMCACHED_NOT_YET_IMPLEMTED           = [31110, HTTP_INTERNAL, "Not yet implemented operations"]
      end
    end
  end
end
