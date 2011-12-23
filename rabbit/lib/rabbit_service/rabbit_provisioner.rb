# Copyright (c) 2009-2011 VMware, Inc.
require 'rabbit_service/common'

class VCAP::Services::Rabbit::Provisioner < VCAP::Services::Base::Provisioner

  include VCAP::Services::Rabbit::Common

  def node_score(node)
    node['available_memory']
  end

end
