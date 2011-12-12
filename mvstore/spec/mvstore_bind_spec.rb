# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require "spec_helper"
require "rest_client"
require "json"

describe "mvstore_node bind" do

  before :all do
    EM.run do
      @app_id = "myapp"
      @opts = get_node_config()
      @logger = @opts[:logger]

      @node = Node.new(@opts)
      @resp = @node.provision("free")

      EM.add_timer(1) do
        @bind_resp = @node.bind(@resp['name'], 'rw')
        EM.add_timer(1) do
          EM.stop
        end
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

  it "should be able to connect to mvstore" do
    is_port_open?(@resp['host'], @resp['port']).should be_true
  end

  it "should return error when tring to bind on non-existed instance" do
    e = nil
    begin
      @node.bind('non-existed', 'rw')
    rescue => e
    end
    e.should_not be_nil
  end

  it "should allow authorized user to access the instance" do
    EM.run do
      url = "http://#{@bind_resp['username']}:#{@bind_resp['password']}@#{@resp['host']}:#{@resp['port']}/db?q=#{CGI::escape("SELECT *;")}&i=mvsql&o=json"
      response = RestClient.get url
      JSON.parse(response).size().should >= 1
      EM.stop
    end
  end

  it "should not allow unauthorized user to access the instance" do
    EM.run do
      url = "http://#{@bind_resp['username']}:boguspw@#{@resp['host']}:#{@resp['port']}/db?q=#{CGI::escape("SELECT * FROM mv:ClassOfClasses;")}&i=mvsql&o=json"
      begin
        response = RestClient.get url
      rescue => e
      end
      e.should_not be_nil
      EM.stop
    end
  end

  it "should return error when trying to unbind a non-existed service" do
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

  it "should be able to unbind it" do
    EM.run do
      resp  = @node.unbind(@bind_resp)
      resp.should be_true
      EM.add_timer(1) do
        EM.stop
      end
    end
  end
end
