# Copyright (c) 2009-2011 VMware, Inc.
require 'spec_helper'
require 'logger'
require 'yajl'
require 'mysql_service/provisioner'

class VCAP::Services::Mysql::Provisioner
  attr_reader :nodes
end

class VCAP::Services::Mysql::Gateway
  def default_config_file
    File.join(File.dirname(__FILE__), '../config/mysql_gateway_test.yml')
  end
end

describe 'Mysql Provisioner Test', components: [:nats]  do

  before :each do
    @nodeopts = getNodeTestConfig
    @opts = getProvisionerTestConfig
    EM.run do
      @provisioner = VCAP::Services::Mysql::Provisioner.new(@opts)
      EM.stop
    end
  end

  it "should remember node announcement" do
    EM.run do
      msg1 = {:id => 'node1'}
      msg2 = {:id => 'node2'}
      @provisioner.on_announce(Yajl::Encoder.encode(msg1))
      @provisioner.on_announce(Yajl::Encoder.encode(msg2))
      @provisioner.nodes.size.should == 2
      EM.stop
    end
  end

  it "should not save duplicated announcement" do
    EM.run do
      msg = {:id => 'node1'}
      @provisioner.on_announce(Yajl::Encoder.encode(msg))
      @provisioner.on_announce(Yajl::Encoder.encode(msg))
      @provisioner.on_announce(Yajl::Encoder.encode(msg))
      @provisioner.nodes.size.should == 1
      EM.stop
    end
  end

  it "should handle malformed announcement msg" do
    EM.run do
      msg = {}
      @provisioner.on_announce(Yajl::Encoder.encode(msg))
      @provisioner.nodes.size.should == 0
      EM.stop
    end
  end

  it "should define score node method" do
    @provisioner.respond_to?("node_score").should be true
    expect {@provisioner.node_score(nil)}.to_not raise_error
    res = @provisioner.node_score({'available_capacity' => 5})
    res.should == 5
  end

  describe "#varz_details" do
    it 'returns everything super returns plus the max_capacity, used_capacity and available_capacity' do
      EM.run do
        msg1 = {
          "id" => "node-1",
          "plan" => "free",
          "available_capacity" => 195,
          "max_capacity" => 200,
          "capacity_unit" => 1,
          "supported_versions" => ["1.0"],
          "time" => Time.now.to_i
        }
        msg2 = {
          "id" => "node-2",
          "plan" => "free",
          "available_capacity" => 900,
          "max_capacity" => 1000,
          "capacity_unit" => 1,
          "supported_versions" => ["1.0"],
          "time" => Time.now.to_i
        }
        msg3 = {
          "id" => "node-3",
          "plan" => "expensive",
          "available_capacity" => 2,
          "max_capacity" => 20,
          "capacity_unit" => 1,
          "supported_versions" => ["1.0"],
          "time" => Time.now.to_i
        }
        @provisioner.on_announce(Yajl::Encoder.encode(msg1))
        @provisioner.on_announce(Yajl::Encoder.encode(msg2))
        @provisioner.on_announce(Yajl::Encoder.encode(msg3))
        EM.stop
      end
      varz_details = @provisioner.varz_details
      varz_details[:plans][0].fetch(:max_capacity).should == 1200
      varz_details[:plans][0].fetch(:used_capacity).should == 105
      varz_details[:plans][0].fetch(:available_capacity).should == 1095
      varz_details[:plans][1].fetch(:max_capacity).should == 20
      varz_details[:plans][1].fetch(:used_capacity).should == 18
      varz_details[:plans][1].fetch(:available_capacity).should == 2
    end
  end

end
