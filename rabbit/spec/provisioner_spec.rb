# Copyright (c) 2009-2011 VMware, Inc.
require File.dirname(__FILE__) + '/spec_helper'

require 'rabbit_service/rabbit_provisioner'

describe VCAP::Services::Rabbit::Provisioner do
  before :all do
    logger = Logger.new(STDOUT, "daily")
    logger.level = Logger::DEBUG
    EM.run do
      @provisioner = VCAP::Services::Rabbit::Provisioner.new({:logger => logger})
      EM.add_timer(1) {EM.stop}
    end
  end

  describe 'Provisioner.node_score' do
    it "should return the node available memory when get the node score" do
      @provisioner.node_score({"available_memory" => 1024}).should == 1024
    end
  end
end
