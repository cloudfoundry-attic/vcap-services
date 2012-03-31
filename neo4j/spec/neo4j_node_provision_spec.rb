# Copyright (c) 2009-2011 VMware, Inc.
require "spec_helper"
require "neo4j_service/neo4j_node"
require "rest-client"

include VCAP::Services::Neo4j


module VCAP
  module Services
    module Neo4j
      class Node
      end
    end
  end
end

describe VCAP::Services::Neo4j::Node do

  before :all do
    EM.run do
      @opts = get_node_config()
      @logger = @opts[:logger]

      @node = Node.new(@opts)
      EM.add_timer(2) { @resp = @node.provision("free") }
      EM.add_timer(4) { EM.stop }
    end
  end

  after :all do
    EM.run do
      begin
        @node.shutdown()
        EM.stop
      rescue
      end
    end
    FileUtils.rm_rf(File.dirname(@opts[:base_dir]))
  end

  it "should have valid response" do
    @resp.should_not be_nil
    inst_name = @resp['name']
    inst_name.should_not be_nil
    inst_name.should_not == ""
  end

  it "should be able to connect to neo4j" do
    is_port_open?('127.0.0.1', @resp['port']).should be_true
  end

  it "should not allow unauthorized user to access the instance" do
    EM.run do
      begin
        neo4j_connect(nil,nil);
      rescue Exception => e
        @logger.debug e
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
        neo4j_connect(nil,nil);
      rescue => e
      end
      e.should_not be_nil
      EM.stop
    end
  end

end


