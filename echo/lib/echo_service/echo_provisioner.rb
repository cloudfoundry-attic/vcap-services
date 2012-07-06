# Copyright (c) 2009-2011 VMware, Inc.
require 'echo_service/common'

class VCAP::Services::Echo::Provisioner < VCAP::Services::Base::Provisioner

  include VCAP::Services::Echo::Common

end
