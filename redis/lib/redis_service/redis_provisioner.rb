# Copyright (c) 2009-2011 VMware, Inc.
$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..', 'base', 'lib')
require 'base/provisioner'
require 'redis_service/common'

class VCAP::Services::Redis::Provisioner < VCAP::Services::Base::Provisioner

  include VCAP::Services::Redis::Common

  def node_score(node)
    node['available_memory']
  end

end
