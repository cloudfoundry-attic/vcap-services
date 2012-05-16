# Copyright (c) 2009-2011 VMware, Inc.
require "spec_helper"

describe "elasticsearch_node bind" do

  before :all do
    EM.run do
      @opts = get_node_config
      @logger = @opts[:logger]
      @node = Node.new(@opts)
      EM.add_timer(1) { @resp = @node.provision("free") }
      EM.add_timer(9) do
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

  it "should be able to connect to elasticsearch" do
    is_port_open?(@bind_resp['host'], @bind_resp['port']).should be_true
    response = RestClient.get "http://#{@bind_resp['host']}:#{@bind_resp['port']}"
    response.should_not be_nil
    response.code.should == 200
  end

  it "should allow authorized user to access the instance" do
    response = RestClient.put("#{@bind_resp['url']}/foo", {})
    response.code.should == 200
    response = RestClient.put("#{@bind_resp['url']}/foo/bar/1", { "message" => "blah blah" }.to_json)
    response.code.should == 201
    response = RestClient.get "#{@bind_resp['url']}/foo/bar/_search"
    response.code.should == 200
  end

  it "should not allow unauthorized user to access the instance" do
    e = nil
    begin
      RestClient.get "http://foo:bar@#{@bind_resp['host']}:#{@bind_resp['port']}/foo/bar/_search"
    rescue => e
    end
    e.should_not be_nil
    e.class.should == RestClient::Unauthorized
  end

  it "should return error when tring to bind on non-existent instance" do
    e = nil
    begin
      @node.bind('non-existent', 'rw')
    rescue => e
    end
    e.should_not be_nil
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
      @node.unprovision(@resp['name'])
      is_port_open?(@resp['host'], @resp['port']).should_not be_true
      EM.stop
    end
  end

  after:all do
    EM.run do
      begin
        @node.shutdown()
        EM.stop
      rescue
      end
    end
    FileUtils.rm_rf(File.dirname(@opts[:base_dir]))
  end

end
