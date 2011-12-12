# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require "spec_helper"
require "rest_client"
require "json"

describe "mvstore_node provision" do

  before :all do
    EM.run do
      @opts = get_node_config()
      @logger = @opts[:logger]
      @node = Node.new(@opts)
      @original_memory = @node.available_memory

      EM.add_timer(2) { @resp = @node.provision("free") }
      EM.add_timer(4) { EM.stop }
    end
  end

  it "should have valid response" do
    @resp.should_not be_nil
    inst_name = @resp['name']
    inst_name.should_not be_nil
    inst_name.should_not == ""
  end

  it "should consume node's memory" do
    (@original_memory - @node.available_memory).should > 0
  end

  it "should be able to connect to mvstore" do
    is_port_open?(@resp['host'], @resp['port']).should be_true
  end

  it "should return varz" do
    EM.run do
      stats = nil
      10.times do
        stats = @node.varz_details
        @node.healthz_details
      end
      stats.should_not be_nil
      stats[:running_services].length.should > 0
      stats[:running_services][0]['name'].should_not be_nil
      stats[:disk].should_not be_nil
      stats[:services_max_memory].should > 0
      stats[:services_used_memory].should > 0
      EM.stop
    end
  end

  it "should return healthz" do
    EM.run do
      stats = @node.healthz_details
      stats.should_not be_nil
      stats[:self].should == "ok"
      stats[@resp['name'].to_sym].should == "ok"
      EM.stop
    end
  end

  it "should allow authorized user to access the instance" do
    EM.run do
      urlbase = "http://#{@resp['username']}:#{@resp['password']}@#{@resp['host']}:#{@resp['port']}/db"
      url = "#{urlbase}?q=#{CGI::escape("SELECT * FROM mv:ClassOfClasses;")}&i=mvsql&o=json"
      response = RestClient.get url
      url = "#{urlbase}?q=#{CGI::escape("INSERT cf_unittest_somevalue=1;")}&i=mvsql&o=json"
      response = RestClient.get url
      JSON.parse(response).size().should == 1
      EM.stop
    end
  end

  it "should not allow unauthorized user to access the instance" do
    EM.run do
      url = "http://#{@resp['username']}:boguspw@#{@resp['host']}:#{@resp['port']}/db?q=#{CGI::escape("SELECT * FROM mv:ClassOfClasses;")}&i=mvsql&o=json"
      begin
        response = RestClient.get url
      rescue Exception => e
        @logger.debug e
      end
      e.should_not be_nil
      EM.stop
    end
  end

  it "should keep the result after node restart" do
    port_open_1 = nil
    port_open_2 = nil
    EM.run do
      EM.add_timer(0) { @node.shutdown }
      EM.add_timer(1) { port_open_1 = is_port_open?(@resp['host'], @resp['port']) }
      EM.add_timer(2) { @node = Node.new(@opts) }
      EM.add_timer(3) { port_open_2 = is_port_open?(@resp['host'], @resp['port']) }
      EM.add_timer(4) { EM.stop }
    end

    begin
      port_open_1.should be_false
      port_open_2.should be_true
      urlbase = "http://#{@resp['username']}:#{@resp['password']}@#{@resp['host']}:#{@resp['port']}/db"
      url = "#{urlbase}?q=#{CGI::escape("SELECT * WHERE EXISTS(cf_unittest_somevalue);")}&i=mvsql&o=json"
      response = RestClient.get url
      JSON.parse(response).size().should >= 1
    rescue => e
    end
  end

  it "should return error when unprovisioning a non-existent instance" do
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

  it "should report error when admin users are deleted from mvstore" do
    EM.run do
      delete_admin(@resp)
      stats = @node.varz_details
      stats.should_not be_nil
      stats[:running_services].length.should > 0
      EM.stop
    end
  end

  # unprovision here
  it "should be able to unprovision an existing instance" do
    EM.run do
      @node.unprovision(@resp['name'], [])

      e = nil
      url = "http://#{@resp['username']}:#{@resp['password']}@#{@resp['host']}:#{@resp['port']}/db?q=#{CGI::escape("SELECT * FROM mv:ClassOfClasses;")}&i=mvsql&o=json"
      begin
        response = RestClient.get url
      rescue Exception => e
        @logger.debug e
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
