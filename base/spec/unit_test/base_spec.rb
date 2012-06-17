# Copyright (c) 2009-2011 VMware, Inc.
require 'helper/spec_helper'
require 'eventmachine'

describe BaseTests do

  it "should connect to node message bus" do
    base = nil
    EM.run do
      Do.at(0) { base = BaseTests.create_base }
      Do.at(1) { EM.stop }
    end
    base.node_mbus_connected.should be_true
  end

end

