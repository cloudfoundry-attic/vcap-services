# Copyright (c) 2009-2011 VMware, Inc.
require "base/provisioner"
require "couchdb_service/common"


class VCAP::Services::CouchDB::Provisioner < VCAP::Services::Base::Provisioner

  include VCAP::Services::CouchDB::Common

  def node_score(node)
    node['available_memory']
  end
end

