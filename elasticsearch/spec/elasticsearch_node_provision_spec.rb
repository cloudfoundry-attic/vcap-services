# Copyright (c) 2009-2011 VMware, Inc.
require "spec_helper"

describe "elasticsearch_node provision" do

  before :all do
    EM.run do
      @opts = get_node_config()
      @logger = @opts[:logger]
      @node = Node.new(@opts)
      EM.add_timer(1) { @resp = @node.provision("free") }
      EM.add_timer(9) { EM.stop }
    end
  end

  it "should have valid response" do
    @resp.should_not be_nil
    inst_name = @resp['name']
    inst_name.should_not be_nil
    inst_name.should_not == ""
  end

  it "should be able to connect to elasticsearch gateway" do
    is_port_open?(@resp['host'], @resp['port']).should be_true
  end

  it "should return varz" do
    EM.run do
      stats = nil
      10.times do
        stats = @node.varz_details
      end
      stats.should_not be_nil
      stats[:running_services].length.should > 0
      stats[:running_services][0]['name'].should_not be_nil
      stats[:running_services][0]['health'].should_not be_nil
      stats[:running_services][0]['health'].has_key?('status').should be_true
      stats[:running_services][0]['index'].should_not be_nil
      stats[:running_services][0]['index'].has_key?('store').should be_true
      stats[:running_services][0]['process'].should_not be_nil
      stats[:running_services][0]['process'].has_key?('id').should be_true
      stats[:disk].should_not be_nil
      stats[:max_capacity].should > 0
      stats[:available_capacity].should > 0
      stats[:instances].length.should > 0
      EM.stop
    end
  end

  it "should keep the result after node restart" do
    port_open_1 = nil, port_open_2 = nil
    EM.run do
      EM.add_timer(0) { @node.shutdown }
      EM.add_timer(1) { port_open_1 = is_port_open?(@resp['host'], @resp['port']) }
      EM.add_timer(3) { @node = Node.new(@opts) }
      EM.add_timer(10) do
        port_open_2 = is_port_open?(@resp['host'], @resp['port'])
        EM.stop
      end
    end
    port_open_1.should be_false
    port_open_2.should be_true
  end

  it "should return error when unprovisioning a non-existent instance" do
    EM.run do
      e = nil
      begin
        @node.unprovision('not existent')
      rescue => e
      end
      e.should_not be_nil
      EM.stop
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
