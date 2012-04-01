# Copyright (c) 2009-2011 VMware, Inc.
require 'helper/spec_helper'
require 'eventmachine'

describe NodeTests do

  it "should announce on startup" do
    node = nil
    provisioner = nil
    EM.run do
      # start provisioner then node
      Do.at(0) { provisioner = NodeTests.create_provisioner }
      Do.at(1) { node = NodeTests.create_node }
      Do.at(2) { EM.stop }
    end
    provisioner.got_announcement.should be_true
  end

  it "should call varz" do
    node = nil
    provisioner = nil
    EM.run do
      # start provisioner then node
      Do.at(0) { provisioner = NodeTests.create_provisioner }
      Do.at(1) { node = NodeTests.create_node }
      Do.at(12) { EM.stop }
    end
    node.varz_invoked.should be_true
  end

  it "should report healthz ok" do
    node = nil
    provisioner = nil
    EM.run do
      # start provisioner then node
      Do.at(0) { provisioner = NodeTests.create_provisioner }
      Do.at(1) { node = NodeTests.create_node }
      Do.at(12) { EM.stop }
    end
    node.healthz_ok.should == "ok\n"
  end

  it "should announce on request" do
    node = nil
    provisioner = nil
    EM.run do
      # start node then provisioner
      Do.at(0) { node = NodeTests.create_node }
      Do.at(1) { provisioner = NodeTests.create_provisioner }
      Do.at(2) { EM.stop }
    end
    node.announcement_invoked.should be_true
    provisioner.got_announcement.should be_true
  end

  it "should announce on identical plan" do
    node = nil
    provisioner = nil
    EM.run do
      # start node then provisioner
      Do.at(0) { node = NodeTests.create_node(:plan => "free") }
      Do.at(1) { provisioner = NodeTests.create_provisioner }
      Do.at(2) { provisioner.send_discover_by_plan("free") }
      Do.at(3) { EM.stop }
    end
    provisioner.got_announcement_by_plan.should be_true
  end

  it "should not announce on different plan" do
    node = nil
    provisioner = nil
    EM.run do
      # start node then provisioner
      Do.at(0) { node = NodeTests.create_node(:plan => "free") }
      Do.at(1) { provisioner = NodeTests.create_provisioner }
      Do.at(2) { provisioner.send_discover_by_plan("nonfree") }
      Do.at(3) { EM.stop }
    end
    provisioner.got_announcement_by_plan.should be_false
  end

  it "should not announce if not ready" do
    node = nil
    provisioner = nil
    EM.run do
      # start provisioner then node
      Do.at(0) { node = NodeTests.create_node; node.set_ready(false) }
      Do.at(1) { provisioner = NodeTests.create_provisioner }
      Do.at(2) { EM.stop }
    end
    provisioner.got_announcement.should be_false
  end

  it "should support concurrent provision" do
    node = nil
    provisioner = nil
    EM.run do
      # start node then provisioner
      Do.sec(0) { node = NodeTests.create_node }
      Do.sec(1) { provisioner = NodeTests.create_provisioner }
      # Start 5 concurrent provision requests, each of which takes 5 seconds to finish
      # Non-concurrent provision handler won't finish in 10 seconds
      Do.sec(2) { 5.times { provisioner.send_provision_request } }
      Do.sec(20) { EM.stop }
    end
    node.provision_invoked.should be_true
    node.provision_times.should == 5
    provisioner.got_provision_response.should be_true
  end

  it "should handle error in node provision" do
    node = nil
    provisioner = nil
    EM.run do
      # start node then provisioner
      Do.sec(0) { node = NodeTests.create_error_node }
      Do.sec(1) { provisioner = NodeTests.create_error_provisioner}
      Do.sec(2) { provisioner.send_provision_request }
      Do.sec(20) { EM.stop }
    end
    node.provision_invoked.should be_true
    provisioner.response.should =~ /Service unavailable/
  end

  it "should decrease capacity after successful provision" do
    node = nil
    provisioner = nil
    original_capacity = 0
    EM.run do
      Do.sec(0) { node = NodeTests.create_node; original_capacity = node.capacity }
      Do.sec(1) { provisioner = NodeTests.create_provisioner}
      Do.sec(2) { provisioner.send_provision_request }
      Do.sec(10) { EM.stop }
    end
    (original_capacity - node.capacity).should > 0
  end

  it "should not decrease capacity after erroneous provision" do
    node = nil
    provisioner = nil
    original_capacity = 0
    EM.run do
      Do.sec(0) { node = NodeTests.create_error_node; original_capacity = node.capacity }
      Do.sec(1) { provisioner = NodeTests.create_provisioner}
      Do.sec(2) { provisioner.send_provision_request }
      Do.sec(10) { EM.stop }
    end
    (original_capacity - node.capacity).should == 0
  end

  it "should support unprovision" do
    node = nil
    provisioner = nil
    EM.run do
      # start node then provisioner
      Do.at(0) { node = NodeTests.create_node }
      Do.at(1) { provisioner = NodeTests.create_provisioner }
      Do.at(2) { provisioner.send_unprovision_request }
      Do.at(20) { EM.stop }
    end
    node.unprovision_invoked.should be_true
  end

  it "should handle error in unprovision" do
    node = nil
    provisioner = nil
    EM.run do
      # start node then provisioner
      Do.at(0) { node = NodeTests.create_error_node }
      Do.at(1) { provisioner = NodeTests.create_error_provisioner }
      Do.at(2) { provisioner.send_unprovision_request }
      Do.at(20) { EM.stop }
    end
    node.unprovision_invoked.should be_true
    provisioner.response.should =~ /Service unavailable/
  end

  it "should increase capacity after successful unprovision" do
    node = nil
    provisioner = nil
    original_capacity = 0
    EM.run do
      Do.sec(0) { node = NodeTests.create_node; original_capacity = node.capacity }
      Do.sec(1) { provisioner = NodeTests.create_provisioner }
      Do.sec(2) { provisioner.send_unprovision_request }
      Do.sec(10) { EM.stop }
    end
    (original_capacity - node.capacity).should < 0
  end

  it "should not increase capacity after erroneous unprovision" do
    node = nil
    provisioner = nil
    original_capacity = 0
    EM.run do
      Do.sec(0) { node = NodeTests.create_error_node; original_capacity = node.capacity }
      Do.sec(1) { provisioner = NodeTests.create_provisioner }
      Do.sec(2) { provisioner.send_unprovision_request }
      Do.sec(10) { EM.stop }
    end
    (original_capacity - node.capacity).should == 0
  end

  it "should support bind" do
    node = nil
    provisioner = nil
    EM.run do
      # start node then provisioner
      Do.at(0) { node = NodeTests.create_node }
      Do.at(1) { provisioner = NodeTests.create_provisioner }
      Do.at(2) { provisioner.send_bind_request }
      Do.at(20) { EM.stop }
    end
    node.bind_invoked.should be_true
  end

  it "should handle error in bind" do
    node = nil
    provisioner = nil
    EM.run do
      # start node then provisioner
      Do.at(0) { node = NodeTests.create_error_node }
      Do.at(1) { provisioner = NodeTests.create_error_provisioner }
      Do.at(2) { provisioner.send_bind_request }
      Do.at(20) { EM.stop }
    end
    node.bind_invoked.should be_true
    provisioner.response.should =~ /Service unavailable/
  end

  it "should support unbind" do
    node = nil
    provisioner = nil
    EM.run do
      # start node then provisioner
      Do.at(0) { node = NodeTests.create_node }
      Do.at(1) { provisioner = NodeTests.create_provisioner }
      Do.at(2) { provisioner.send_unbind_request }
      Do.at(20) { EM.stop }
    end
    node.unbind_invoked.should be_true
  end

  it "should handle error in unbind" do
    node = nil
    provisioner = nil
    EM.run do
      # start node then provisioner
      Do.at(0) { node = NodeTests.create_error_node }
      Do.at(1) { provisioner = NodeTests.create_error_provisioner }
      Do.at(2) { provisioner.send_unbind_request }
      Do.at(20) { EM.stop }
    end
    node.unbind_invoked.should be_true
    provisioner.response.should =~ /Service unavailable/
  end

  it "should support restore" do
    node = nil
    provisioner = nil
    EM.run do
      # start node then provisioner
      Do.at(0) { node = NodeTests.create_node }
      Do.at(1) { provisioner = NodeTests.create_provisioner }
      Do.at(2) { provisioner.send_restore_request }
      Do.at(20) { EM.stop }
    end
    node.restore_invoked.should be_true
  end

  it "should handle error in restore" do
    node = nil
    provisioner = nil
    EM.run do
      # start node then provisioner
      Do.at(0) { node = NodeTests.create_error_node }
      Do.at(1) { provisioner = NodeTests.create_error_provisioner }
      Do.at(2) { provisioner.send_restore_request }
      Do.at(20) { EM.stop }
    end
    node.restore_invoked.should be_true
    provisioner.response.should =~ /Service unavailable/
  end

  it "should support disable instance" do
    node = nil
    provisioner = nil
    EM.run do
      # start node then provisioner
      Do.at(0) { node = NodeTests.create_node }
      Do.at(1) { provisioner = NodeTests.create_provisioner }
      Do.at(2) { provisioner.send_disable_request }
      Do.at(3) { EM.stop }
    end
    node.disable_invoked.should be_true
  end

  it "should handle error in disable instance" do
    node = nil
    provisioner = nil
    EM.run do
      # start node then provisioner
      Do.at(0) { node = NodeTests.create_error_node }
      Do.at(1) { provisioner = NodeTests.create_error_provisioner }
      Do.at(2) { provisioner.send_disable_request }
      Do.at(3) { EM.stop }
    end
    node.disable_invoked.should be_true
    provisioner.response.should =~ /Service unavailable/
  end

  it "should support enable instance" do
    node = nil
    provisioner = nil
    EM.run do
      # start node then provisioner
      Do.at(0) { node = NodeTests.create_node }
      Do.at(1) { provisioner = NodeTests.create_provisioner }
      Do.at(2) { provisioner.send_enable_request }
      Do.at(3) { EM.stop }
    end
    node.enable_invoked.should be_true
  end

  it "should handle error in enable instance" do
    node = nil
    provisioner = nil
    EM.run do
      # start node then provisioner
      Do.at(0) { node = NodeTests.create_error_node }
      Do.at(1) { provisioner = NodeTests.create_error_provisioner }
      Do.at(2) { provisioner.send_enable_request }
      Do.at(3) { EM.stop }
    end
    node.enable_invoked.should be_true
    provisioner.response.should =~ /Service unavailable/
  end

  it "should support import instance" do
    node = nil
    provisioner = nil
    EM.run do
      # start node then provisioner
      Do.at(0) { node = NodeTests.create_node }
      Do.at(1) { provisioner = NodeTests.create_provisioner }
      Do.at(2) { provisioner.send_import_request }
      Do.at(3) { EM.stop }
    end
    node.import_invoked.should be_true
  end

  it "should handle error in import instance" do
    node = nil
    provisioner = nil
    EM.run do
      # start node then provisioner
      Do.at(0) { node = NodeTests.create_error_node }
      Do.at(1) { provisioner = NodeTests.create_error_provisioner }
      Do.at(2) { provisioner.send_import_request }
      Do.at(3) { EM.stop }
    end
    node.import_invoked.should be_true
    provisioner.response.should =~ /Service unavailable/
  end

  it "should support update instance" do
    node = nil
    provisioner = nil
    EM.run do
      # start node then provisioner
      Do.at(0) { node = NodeTests.create_node }
      Do.at(1) { provisioner = NodeTests.create_provisioner }
      Do.at(2) { provisioner.send_update_request }
      Do.at(3) { EM.stop }
    end
    node.update_invoked.should be_true
  end

  it "should handle error in update instance" do
    node = nil
    provisioner = nil
    EM.run do
      # start node then provisioner
      Do.at(0) { node = NodeTests.create_error_node }
      Do.at(1) { provisioner = NodeTests.create_error_provisioner }
      Do.at(2) { provisioner.send_update_request }
      Do.at(3) { EM.stop }
    end
    node.update_invoked.should be_true
    provisioner.response.should =~ /Service unavailable/
  end

  it "should support cleanupnfs instance" do
    node = nil
    provisioner = nil
    EM.run do
      # start node then provisioner
      Do.at(0) { node = NodeTests.create_node }
      Do.at(1) { provisioner = NodeTests.create_provisioner }
      Do.at(2) { provisioner.send_cleanupnfs_request }
      Do.at(3) { EM.stop }
    end
    provisioner.got_cleanupnfs_response.should be_true
  end

  it "should decrease capacity after successful migration" do
    node = nil
    provisioner = nil
    original_capacity = 0
    EM.run do
      Do.sec(0) { node = NodeTests.create_node; original_capacity = node.capacity }
      Do.sec(1) { provisioner = NodeTests.create_provisioner}
      Do.sec(2) { provisioner.send_update_request }
      Do.sec(3) { EM.stop }
    end
    (original_capacity - node.capacity).should == 1
  end

  it "should not decrease capacity after erroneous migration" do
    node = nil
    provisioner = nil
    original_capacity = 0
    EM.run do
      Do.sec(0) { node = NodeTests.create_error_node; original_capacity = node.capacity }
      Do.sec(1) { provisioner = NodeTests.create_provisioner}
      Do.sec(2) { provisioner.send_update_request }
      Do.sec(3) { EM.stop }
    end
    (original_capacity - node.capacity).should == 0
  end

  it "should support check_orphan when no handles" do
    node = nil
    provisioner = nil
    EM.run do
      # start node then provisioner
      Do.at(0) { node = NodeTests.create_node}
      Do.at(1) { provisioner = NodeTests.create_provisioner }
      Do.at(2) { provisioner.send_check_orphan_request }
      Do.at(5) { EM.stop }
    end
    provisioner.ins_hash[TEST_NODE_ID].count.should == 0
    provisioner.bind_hash[TEST_NODE_ID].count.should == 0
  end

  it "should support check_orphan when node has massive instances" do
    node = nil
    provisioner = nil
    EM.run do
      # start node then provisioner
      Do.at(0) { node = NodeTests.create_node(:ins_count => 1024 * 128, :bind_count => 1024)}
      Do.at(1) { provisioner = NodeTests.create_provisioner }
      Do.at(2) { provisioner.send_check_orphan_request }
      Do.at(30) { EM.stop }
    end
    provisioner.ins_hash[TEST_NODE_ID].count.should == 1024 * 128
    provisioner.bind_hash[TEST_NODE_ID].count.should == 1024
  end

  it "should support check_orphan when node has massive bindings" do
    node = nil
    provisioner = nil
    EM.run do
      # start node then provisioner
      Do.at(0) { node = NodeTests.create_node(:ins_count => 1024, :bind_count => 1024 * 64)}
      Do.at(1) { provisioner = NodeTests.create_provisioner }
      Do.at(2) { provisioner.send_check_orphan_request }
      Do.at(30) { EM.stop }
    end
    provisioner.ins_hash[TEST_NODE_ID].count.should == 1024
    provisioner.bind_hash[TEST_NODE_ID].count.should == 1024 * 64
  end

  it "should support check_orphan when node has massive handles" do
    node = nil
    provisioner = nil
    EM.run do
      # start node then provisioner
      Do.at(0) { node = NodeTests.create_node(:ins_count => 1024 * 128, :bind_count => 1024 * 16)}
      Do.at(1) { provisioner = NodeTests.create_provisioner }
      Do.at(2) { provisioner.send_check_orphan_request }
      Do.at(45) { EM.stop }
    end
    provisioner.ins_hash[TEST_NODE_ID].count.should == 1024 * 128
    provisioner.bind_hash[TEST_NODE_ID].count.should == 1024 * 16
  end

  it "should support purge_orphan" do
    node = nil
    provisioner = nil
    EM.run do
      # start node then provisioner
      Do.at(0) { node = NodeTests.create_node }
      Do.at(1) { provisioner = NodeTests.create_provisioner }
      Do.at(2) { provisioner.send_purge_orphan_request }
      Do.at(5) { EM.stop }
    end
    node.unprovision_count.should == 2
    node.unbind_count.should == 2
  end
end
