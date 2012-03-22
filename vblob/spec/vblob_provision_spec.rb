# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require "spec_helper"

describe "vblob_node provision" do

  before :all do
    EM.run do
      @opts = get_node_config()
      @logger = @opts[:logger]
      @node = Node.new(@opts)

      EM.add_timer(2) { @resp = @node.provision("free") }
      EM.add_timer(4) { EM.stop }
    end
  end

  it "should have valid response" do
    @resp.should_not be_nil
    puts @resp
    inst_name = @resp['name']
    inst_name.should_not be_nil
    inst_name.should_not == ""
  end

  it "should be able to connect to vblob gateway" do
    is_port_open?('127.0.0.1',@resp['port']).should be_true
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
      stats[:max_capacity].should > 0
      stats[:available_capacity].should > 0
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

  it "should keep the result after node restart" do
    port_open_1 = nil
    port_open_2 = nil
    EM.run do
      EM.add_timer(0) { @node.shutdown }
      EM.add_timer(1) { port_open_1 = is_port_open?('127.0.0.1', @resp['port'])
                      }
      EM.add_timer(2) { @node = Node.new(@opts) }
      EM.add_timer(3) { port_open_2 = is_port_open?('127.0.0.1', @resp['port'])
                      }
      EM.add_timer(4) { EM.stop }
    end

    port_open_1.should be_false
    port_open_2.should be_true
  end

  it "should return error when unprovisioning a non-existent instance" do
    EM.run do
      e = nil
      begin
        @node.unprovision('not existent', [])
      rescue => e
      end
      e.should_not be_nil
      EM.stop
    end
  end

  # unprovision here
  it "should be able to unprovision an existent instance" do
    EM.run do
      e = nil
      begin
        @node.unprovision(@resp['name'], [])
      rescue => e
      end
      e.should be_nil
      EM.stop
    end
  end

  after:all do
    FileUtils.rm_rf Dir.glob('/tmp/vblob')
  end
end


