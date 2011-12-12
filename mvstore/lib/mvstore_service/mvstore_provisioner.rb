# Copyright (c) 2009-2011 VMware, Inc.
require "base/provisioner"
require "mvstore_service/common"

class VCAP::Services::MVStore::Provisioner < VCAP::Services::Base::Provisioner
  include VCAP::Services::MVStore::Common
  def node_score(node)
    node['available_memory']
  end
end
