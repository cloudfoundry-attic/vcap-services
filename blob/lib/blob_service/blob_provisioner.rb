# Copyright (c) 2009-2011 VMware, Inc.
require "blob_service/common"

class VCAP::Services::Blob::Provisioner < VCAP::Services::Base::Provisioner

  include VCAP::Services::Blob::Common

  def node_score(node)
    node['available_memory']
  end
end

