# Copyright (c) 2009-2011 VMware, Inc.
require 'postgresql_service/common'

class VCAP::Services::Postgresql::Provisioner < VCAP::Services::Base::Provisioner

  include VCAP::Services::Postgresql::Common

end
