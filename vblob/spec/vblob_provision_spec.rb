# Copyright (c) 2009-2011 VMware, Inc.

require "spec_helper"

describe "vblob wardenization" do

  before :all do
    EM.run do
      @opts = get_node_config()
      @logger = @opts[:logger]
      @node = Node.new(@opts)
      EM.add_timer(1) { EM.stop }
    end
  end

  after :all do
    @node.shutdown if @node
    FileUtils.rm_rf('/tmp/vblob')
  end

  it "should return 0 for successful 'sh' commands" do
    status = Node.sh "ls /"
    status.should == 0
  end

  it "should raise error for failed 'sh' commands" do
    lambda { Node.sh("ls /abc") }.should raise_error()
  end

  it "should be able to provision with warden" do
    @response = @node.provision("free")
    @response.should_not be_nil
    @response['name'].should_not be_nil
    @response['name'].should_not == ""
    @response['hostname'].should_not be_nil
    @response['host'].should_not be_nil
    @response['port'].should_not be_nil
    @response['username'].should_not be_nil
    @response['password'].should_not be_nil
    @node.unprovision(@response['name'], [])
  end

  context "when a vblob instance was provisioned" do

    before :each do
      @response = @node.provision("free")
      @provisioned_service = @node.get_instance(@response['name'])
    end

    after :each do
      @node.unprovision(@response['name'], [])
    end

    it "should return varz" do
      EM.run do
        stats = nil
        10.times { stats = @node.varz_details }
        stats.should_not be_nil
        stats[:nfs_free_space].should_not == ""
        stats[:max_capacity].should > 0
        stats[:available_capacity].should > 0
        stats[:instances].has_value?("ok").should be_true
        stats[:instances].has_value?("fail").should be_false
        EM.stop
      end
    end

    it "should contain valid container handle and ip address" do
      @provisioned_service['ip'].should_not be_nil
      @provisioned_service['container'].should_not be_nil
    end

    it "should open the port for request" do
      is_port_open?(@provisioned_service.ip, @provisioned_service.service_port).should be_true
    end

    it "should be able enable the instance after disable it" do
      @node.disable_instance(@response, {'' => {'credentials' => ''}}).should be_true
      @node.enable_instance(@response, {'' => {'credentials' => '' }}).should be_true
      @provisioned_service = @node.get_instance(@response['name'])
      is_port_open?(@provisioned_service.ip, @provisioned_service.service_port).should be_true
    end

    it "should raise error when unprovisioning a non-existent instance" do
      expect { @node.unprovision('non-existent', []) }.should raise_error
    end

    it "should keep the result after node restart" do
      @node.shutdown
      is_port_open?(@provisioned_service[:ip], @provisioned_service.service_port).should be_false
      @node.pre_send_announcement
      @provisioned_service = @node.get_instance(@response['name'])
      is_port_open?(@provisioned_service[:ip], @provisioned_service.service_port).should be_true
    end
  end
end
