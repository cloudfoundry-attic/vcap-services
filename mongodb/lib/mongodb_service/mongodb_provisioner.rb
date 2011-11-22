# Copyright (c) 2009-2011 VMware, Inc.
require "mongodb_service/common"

class VCAP::Services::MongoDB::Provisioner < VCAP::Services::Base::Provisioner

  include VCAP::Services::MongoDB::Common

  def node_score(node)
    node['available_memory']
  end
end

