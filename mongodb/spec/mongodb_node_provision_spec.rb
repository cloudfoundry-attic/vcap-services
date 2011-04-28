# Copyright (c) 2009-2011 VMware, Inc.
require "spec_helper"
require "mongodb_service/mongodb_node"
require "mongo"

include VCAP::Services::MongoDB


module VCAP
  module Services
    module MongoDB
      class Node
        attr_reader :available_memory
      end
    end
  end
end

describe VCAP::Services::MongoDB::Node do

  before :all do
    EM.run do
      @opts = get_node_config()
      @logger = @opts[:logger]
      @node = Node.new(@opts)
      @original_memory = @node.available_memory

      @resp = @node.provision("free")

      EM.add_timer(1) do
        EM.stop
      end
    end
  end

  it "should have valid response" do
    @resp.should_not be_nil
    puts @resp
    inst_name = @resp['name']
    inst_name.should_not be_nil
    inst_name.should_not == ""
  end

  it "should consume node's memory" do
    (@original_memory - @node.available_memory).should > 0
  end

  it "should be able to connect to mongodb" do
    is_port_open?('127.0.0.1', @resp['port']).should be_true
  end

  it "should not allow unauthorized user to access the instance" do
    EM.run do
      conn = Mongo::Connection.new('localhost', @resp['port']).db(@resp['db'])
      begin
        coll = conn.collection('mongo_unit_test')
        coll.insert({'a' => 1})
        coll.count()
      rescue Exception => e
        @logger.debug e
      end
      e.should_not be_nil
      EM.stop
    end
  end

  it "should return varz" do
    EM.run do
      stats = @node.varz_details
      stats.should_not be_nil
      stats[:running_services].length.should > 0
      stats[:running_services][0]['name'].should_not be_nil
      stats[:running_services][0]['db'].should_not be_nil
      stats[:disk].should_not be_nil
      stats[:services_max_memory].should > 0
      stats[:services_used_memory].should > 0
      EM.stop
    end
  end

  it "should return error when unprovisioning a non-existed instance" do
    EM.run do
      e = nil
      begin
        @node.unprovision('no existed', [])
      rescue => e
      end
      e.should_not be_nil
      EM.stop
    end
  end

  # unprovision here
  it "should be able to unprovision an existing instance" do
    EM.run do
      @node.unprovision(@resp['name'], [])

      e = nil
      begin
        conn = Mongo::Connection.new('localhost', @resp[:port]).db('local')
      rescue => e
      end
      e.should_not be_nil
      EM.stop
    end
  end

  it "should release memory" do
    EM.run do
      @original_memory.should == @node.available_memory
      EM.stop
    end
  end

end


