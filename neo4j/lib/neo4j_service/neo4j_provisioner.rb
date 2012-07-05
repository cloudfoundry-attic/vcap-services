# Copyright (c) 2009-2011 VMware, Inc.
require "neo4j_service/common"

class VCAP::Services::Neo4j::Provisioner < VCAP::Services::Base::Provisioner

  include VCAP::Services::Neo4j::Common

end

