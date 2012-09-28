# Copyright (c) 2009-2011 VMware, Inc.
# This code is based on Redis as a Service.

module VCAP
  module Services
    module Memcached
      class MemcachedError < VCAP::Services::Base::Error::ServiceError
        # 31200 - 31299  Memcached-specific Error
        MEMCACHED_SAVE_INSTANCE_FAILED        = [31200, HTTP_INTERNAL, "Could not save instance: %s"]
        MEMCACHED_DESTROY_INSTANCE_FAILED     = [31201, HTTP_INTERNAL, "Could not destroy instance: %s"]
        MEMCACHED_FIND_INSTANCE_FAILED        = [31202, HTTP_NOT_FOUND, "Could not find instance: %s"]
        MEMCACHED_START_INSTANCE_FAILED       = [31203, HTTP_INTERNAL, "Could not start instance: %s"]
        MEMCACHED_CLEANUP_INSTANCE_FAILED     = [31204, HTTP_INTERNAL, "Could not cleanup instance, the reasons: %s"]
        MEMCACHED_CONNECT_INSTANCE_FAILED     = [31205, HTTP_INTERNAL, "Could not connect memcached instance"]
        MEMCACHED_SET_INSTANCE_PASS_FAILED    = [31206, HTTP_INTERNAL, "Could not set memcached instance password"]
        MEMCACHED_RESTORE_FILE_NOT_FOUND      = [31207, HTTP_INTERNAL, "Could not find memcached restore data file %s"]
      end
    end
  end
end
