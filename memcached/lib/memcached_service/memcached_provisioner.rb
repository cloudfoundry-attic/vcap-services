# Copyright (c) 2009-2011 VMware, Inc.
# This code is based on Redis as a Service.

require 'memcached_service/common'

class VCAP::Services::Memcached::Provisioner < VCAP::Services::Base::Provisioner

  include VCAP::Services::Memcached::Common
end
