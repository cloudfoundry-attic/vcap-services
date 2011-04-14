require File.dirname(__FILE__) + '/spec_helper'

require 'redis_service/redis_provisioner'

describe VCAP::Services::Redis::Provisioner do
  before :all do
    logger = Logger.new(STDOUT, "daily")
    logger.level = Logger::DEBUG
    EM.run do
      @provisioner = VCAP::Services::Redis::Provisioner.new({:logger => logger})
      EM.add_timer(1) {EM.stop}
    end
  end

  describe 'Provisioner.node_score' do
    it "should returen the node available memory when get the node score" do
      @provisioner.node_score({"available_memory" => 1024}).should == 1024
    end
  end
end
