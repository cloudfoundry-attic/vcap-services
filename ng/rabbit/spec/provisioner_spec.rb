# Copyright (c) 2009-2011 VMware, Inc.
require File.dirname(__FILE__) + '/spec_helper'

require 'rabbit_service/rabbit_provisioner'

describe VCAP::Services::Rabbit::Provisioner do
  before :all do
    logger = Logger.new(STDOUT, "daily")
    logger.level = Logger::DEBUG
    EM.run do
      @provisioner = VCAP::Services::Rabbit::Provisioner.new(
	      {:logger => logger, :cc_api_version => "v1",
	       :plan_management => {:plans => {:free => {:low_water => 10}}}
              })
      EM.add_timer(1) {EM.stop}
    end
  end

  describe 'Provisioner.node_score' do
    it "should return the node available capacity when get the node score" do
      @provisioner.node_score({"available_capacity" => 1024}).should == 1024
    end
  end

  describe 'Provisioner.varz' do
    it "should contain plan config in varz" do
      varz = @provisioner.varz_details
      varz[:plans][0][:plan].should == :free
      varz[:plans][0][:low_water].should == 10
    end
  end
end
