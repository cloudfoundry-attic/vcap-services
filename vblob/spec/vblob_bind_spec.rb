# Copyright (c) 2009-2011 VMware, Inc.
require "spec_helper"

describe "vblob_node bind" do

  before :all do
    EM.run do
      @app_id = "myapp"
      @opts = get_node_config()
      @logger = @opts[:logger]
      @node = Node.new(@opts)
      EM.add_timer(2) do #must! wait for a while before provisioning
        @resp = @node.provision("free")
      end
      EM.add_timer(4) do
        @bind_resp = @node.bind(@resp['name'], 'rw')
        EM.stop
      end
    end
  end

  it "should have valid response" do
    @resp.should_not be_nil
    @resp['host'].should_not be_nil
    @resp['port'].should_not be_nil
    @resp['username'].should_not be_nil
    @resp['password'].should_not be_nil
    @bind_resp.should_not be_nil
    @bind_resp['host'].should_not be_nil
    @bind_resp['port'].should_not be_nil
    @bind_resp['username'].should_not be_nil
    @bind_resp['password'].should_not be_nil
  end

  it "should be able to connect to vblob" do
    is_port_open?('127.0.0.1', @resp['port']).should be_true
  end

  it "should return error when tring to bind on non-existent instance" do
    e = nil
    begin
      @node.bind('non-existent', 'rw')
    rescue => e
    end
    e.should_not be_nil
  end

  it "should allow binded user to access" do
    response = nil
    EM.run do
      begin
        response = `curl http://#{@resp['host']}:#{@resp['port']}/bucket1 -X PUT -s`
        response = `curl http://#{@resp['host']}:#{@resp['port']}/bucket1 -X DELETE -s`
      rescue => e
      end
      e.should be_nil
      EM.stop
    end
    response.should_not be_nil
  end

  it "should return error when trying to unbind a non-existent service" do
    EM.run do
      begin
        resp  = @node.unbind('not existed')
      rescue => e
      end
      e.should be_true
      EM.add_timer(1) do
        EM.stop
      end
    end
  end

  # unbind here
  it "should be able to unbind it" do
    EM.run do
      resp  = @node.unbind(@bind_resp)
      resp.should be_true
      EM.add_timer(1) do
        EM.stop
      end
    end
  end

  # unprovision here
  it "should be able to unprovision an existing instance" do
    EM.run do
      @node.unprovision(@resp['name'], [])

      e = nil
      begin
      is_port_open?('127.0.0.1',@resp['port']).should_not be_true
      rescue => e
      end
      EM.stop
    end
  end

  after:all do
    FileUtils.rm_rf Dir.glob('/tmp/vblob')
  end
end


