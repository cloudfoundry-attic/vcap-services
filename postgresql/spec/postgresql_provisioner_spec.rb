# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require 'spec_helper'
require 'logger'
require 'yajl'
require 'postgresql_service/provisioner'

class VCAP::Services::Postgresql::Provisioner
  attr_reader :nodes
end

class VCAP::Services::Postgresql::Gateway
  def default_config_file
    File.join(File.dirname(__FILE__), '../config/postgresql_gateway_test.yml')
  end
end

describe 'Postgresql Provisioner Test' do

  before :each do
    @nodeopts = getNodeTestConfig
    @opts = getProvisionerTestConfig
    EM.run do
      @provisioner = VCAP::Services::Postgresql::Provisioner.new(@opts)
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
    lambda {@provisioner.node_score(nil)}.should_not raise_error
    res = @provisioner.node_score({'available_capacity' => 5})
    res.should == 5
  end

end
