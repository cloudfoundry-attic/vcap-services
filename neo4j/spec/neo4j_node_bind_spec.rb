# Copyright (c) 2009-2011 VMware, Inc.
require "spec_helper"
require "neo4j_service/neo4j_node"
require "rest-client"

include VCAP::Services::Neo4j


module VCAP
  module Services
    module Neo4j
      class Node
        attr_reader :available_memory
      end
    end
  end
end

describe VCAP::Services::Neo4j::Node do

  before :all do
    EM.run do
      @app_id = "myapp"
      @opts = get_node_config()
      @logger = @opts[:logger]

      @node = Node.new(@opts)
      @resp = @node.provision("free")

      sleep 1
      @bind_resp = @node.bind(@resp['name'],'rw')

      sleep 1
      EM.stop
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
    @bind_resp.should_not be_nil
    @bind_resp['hostname'].should_not be_nil
    @bind_resp['host'].should == @bind_resp['hostname']
    @bind_resp['port'].should_not be_nil
    @bind_resp['name'].should_not be_nil
    @bind_resp['username'].should_not be_nil
    @bind_resp['password'].should_not be_nil
  end

  it "should be able to connect to neo4j" do
    is_port_open?(@resp['host'], @resp['port']).should be_true
  end

  it "should allow authorized user to access the instance" do
    EM.run do
      response = neo4j_connect()
      response.code.should == 200
      result = JSON.parse(response.body)
      result["reference_node"].should_not be_nil
      EM.stop
    end
  end

  it "should allow authorized user to create data in the instance" do
    EM.run do
      url = neo4j_url() + "node"
      response = RestClient.post url, {:accept => :json}
      response.code.should == 201
      node_url = neo4j_url(nil) + "node/"
      response.headers[:location].should =~ /#{node_url}\d+/
      EM.stop
    end
  end

  it "should not allow unauthorized user to access the instance" do
    EM.run do
      begin
        response = neo4j_connect(nil,nil);
        response.code.should == 200
      rescue => e
      end
      e.should_not be_nil
      EM.stop
    end
  end

  it "should not allow valid user with empty password to access the instance" do
    EM.run do
      begin
        response = neo4j_connect(@bind_resp['username'],nil)
      rescue => e
      end
      e.should_not be_nil
      EM.stop
    end
  end

  # unbind here
  it "should be able to unbind it" do
    EM.run do
      resp  = @node.unbind(@bind_resp)
      resp.should be_true
      sleep 1
      EM.stop
    end
  end

  it "should not allow user to access the instance after unbind" do
    EM.run do
      begin
        neo4j_connect()
      rescue => e
        e.should_not be_nil
      end
      EM.stop

    end
  end

  # unprovision here
  it "should be able to unprovision an existing instance" do
    EM.run do
      @node.unprovision(@resp['name'], [])

      e = nil
      begin
        neo4j_connect(nil,nil)
      rescue => e
      end
      e.should_not be_nil
      EM.stop
    end
  end

end
