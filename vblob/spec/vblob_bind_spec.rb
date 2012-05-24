# Copyright (c) 2009-2011 VMware, Inc.

$:.unshift(File.dirname(__FILE__))

require "spec_helper"

describe "vblob_node bind" do

  before :all do
    EM.run do
      @opts = get_node_config()
      @logger = @opts[:logger]
      @node = Node.new(@opts)
      @response = @node.provision("free")
      @provisioned_service = @node.get_instance(@response['name'])
      @bind_response = @node.bind(@response['name'], 'rw')
      EM.add_timer(1) { EM.stop }
    end
  end

  after :all do
    @node.unprovision(@response['name'], [])
    @node.shutdown if @node
    FileUtils.rm_rf('/tmp/vblob')
  end

  it "should be able to bind existing instance" do
    @bind_response.should_not be_nil
    @bind_response['host'].should_not be_nil
    @bind_response['port'].should_not be_nil
    @bind_response['host'].should == @response['hostname']
    @bind_response['host'].should == @bind_response['host']
    @bind_response['port'].should == @response['port']
    @bind_response['hostname'].should_not be_nil
    @bind_response['name'].should_not be_nil
    @bind_response['username'].should_not be_nil
    @bind_response['password'].should_not be_nil
  end

  it "should return error when binding a non-existed instance" do
    expect { @node.bind('non-existed', 'rw') }.should raise_error
  end

  it "should allow binded user to access" do
    lambda {
      response = `curl http://#{@provisioned_service[:ip]}:#{@provisioned_service.service_port} -s`
      response = `curl http://#{@provisioned_service[:ip]}:#{@provisioned_service.service_port}/bucket1 -X PUT -s`
      response = `curl http://#{@provisioned_service[:ip]}:#{@provisioned_service.service_port}/bucket1 -X DELETE -s`
    }.should_not raise_error
  end

  it "should be able to unbind after a successful bind" do
    @node.unbind(@bind_response).should be_true
  end

end
