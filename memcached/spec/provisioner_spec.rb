# Copyright (c) 2009-2011 VMware, Inc.
# This code is based on Redis as a Service.

require File.dirname(__FILE__) + '/spec_helper'

require 'memcached_service/memcached_provisioner'

describe VCAP::Services::Memcached::Provisioner do
  before :all do
    logger = Logger.new(STDOUT, "daily")
    logger.level = Logger::DEBUG
    EM.run do
      @provisioner = VCAP::Services::Memcached::Provisioner.new({:logger => logger})
      EM.add_timer(1) {EM.stop}
    end
  end

  describe 'Provisioner.node_score' do
    it "should return the node available capacity when get the node score" do
      @provisioner.node_score({"available_capacity" => 5}).should == 5
    end
  end
end
