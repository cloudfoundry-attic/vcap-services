# Copyright (c) 2009-2011 VMware, Inc.
require 'spec_helper'

require 'eventmachine'

module Do

  # the tests below do various things then wait for something to
  # happen -- so there's a potential for a race condition.  to
  # minimize the risk of the race condition, increase this value (0.1
  # seems to work about 90% of the time); but to make the tests run
  # faster, decrease it
  STEP_DELAY = 0.5

  def self.at(index, &blk)
    EM.add_timer(index*STEP_DELAY) { blk.call if blk }
  end

  # Respect the real seconds while doing concurrent testing
  def self.sec(index, &blk)
    EM.add_timer(index) { blk.call if blk }
  end

end

describe BaseTests do

  it "should connect to node message bus" do
    base = nil
    EM.run do
      Do.at(0) { base = BaseTests.create_base }
      Do.at(1) { EM.stop }
    end
    base.node_mbus_connected.should be_true
  end

  it "should call varz" do
    base = nil
    EM.run do
      Do.sec(0) { base = BaseTests.create_base }
      # varz is invoked 5 seconds after base is created
      Do.sec(6) { EM.stop }
    end
    base.varz_invoked.should be_true
  end

  it "should call healthz" do
    base = nil
    EM.run do
      Do.sec(0) { base = BaseTests.create_base }
      # healthz is invoked 5 seconds after base is created
      Do.sec(6) { EM.stop }
    end
    base.healthz_invoked.should be_true
  end

end

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
      Do.at(0) { node = NodeTests.create_node(1024 * 128, 1024)}
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
      Do.at(0) { node = NodeTests.create_node(1024, 1024 * 64)}
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
      Do.at(0) { node = NodeTests.create_node(1024 * 128, 1024 * 16)}
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

describe ProvisionerTests do

  it "should autodiscover 1 node when started first" do
    provisioner = nil
    node = nil
    # start provisioner, then node
    EM.run do
      Do.at(0) { provisioner = ProvisionerTests.create_provisioner }
      Do.at(1) { node = ProvisionerTests.create_node(1) }
      Do.at(2) { EM.stop }
    end
    provisioner.node_count.should == 1
  end

  it "should autodiscover 1 node when started second" do
    provisioner = nil
    node = nil
    EM.run do
      # start node, then provisioner
      Do.at(0) { node = ProvisionerTests.create_node(1) }
      Do.at(1) { provisioner = ProvisionerTests.create_provisioner }
      Do.at(2) { EM.stop }
    end
    provisioner.node_count.should == 1
  end

  it "should autodiscover 3 nodes when started first" do
    provisioner = nil
    node1 = nil
    node2 = nil
    node3 = nil
    # start provisioner, then nodes
    EM.run do
      Do.at(0) { provisioner = ProvisionerTests.create_provisioner }
      Do.at(1) { node1 = ProvisionerTests.create_node(1) }
      Do.at(2) { node2 = ProvisionerTests.create_node(2) }
      Do.at(3) { node3 = ProvisionerTests.create_node(3) }
      Do.at(4) { EM.stop }
    end
    provisioner.node_count.should == 3
  end

  it "should autodiscover 3 nodes when started second" do
    provisioner = nil
    node1 = nil
    node2 = nil
    node3 = nil
    EM.run do
      # start nodes, then provisioner
      Do.at(0) { node1 = ProvisionerTests.create_node(1) }
      Do.at(1) { node2 = ProvisionerTests.create_node(2) }
      Do.at(2) { node3 = ProvisionerTests.create_node(3) }
      Do.at(3) { provisioner = ProvisionerTests.create_provisioner }
      Do.at(4) { EM.stop }
    end
    provisioner.node_count.should == 3
  end

  it "should support provision" do
    provisioner = nil
    gateway = nil
    node = nil
    EM.run do
      Do.at(0) { provisioner = ProvisionerTests.create_provisioner }
      Do.at(1) { gateway = ProvisionerTests.create_gateway(provisioner) }
      Do.at(2) { node = ProvisionerTests.create_node(1) }
      Do.at(3) { gateway.send_provision_request }
      Do.at(4) { EM.stop }
    end
    gateway.got_provision_response.should be_true
  end

  it "should handle error in provision" do
    provisioner = nil
    gateway = nil
    node = nil
    EM.run do
      Do.at(0) { provisioner = ProvisionerTests.create_provisioner }
      Do.at(1) { gateway = ProvisionerTests.create_error_gateway(provisioner) }
      Do.at(2) { node = ProvisionerTests.create_error_node(1) }
      Do.at(3) { gateway.send_provision_request }
      Do.at(4) { EM.stop }
    end
    node.got_provision_request.should be_true
    gateway.provision_response.should be_false
    gateway.error_msg['status'].should == 500
    gateway.error_msg['msg']['code'].should == 30500
  end

  it "should pick the best node when provisioning" do
    provisioner = nil
    gateway = nil
    node1 = nil
    node2 = nil
    EM.run do
      Do.at(0) { provisioner = ProvisionerTests.create_provisioner }
      Do.at(1) { gateway = ProvisionerTests.create_gateway(provisioner) }
      Do.at(2) { node1 = ProvisionerTests.create_node(1, 1) }
      Do.at(3) { node2 = ProvisionerTests.create_node(2, 2) }
      Do.at(4) { gateway.send_provision_request }
      Do.at(5) { EM.stop }
    end
    node1.got_provision_request.should be_false
    node2.got_provision_request.should be_true
  end

  it "should support unprovision" do
    provisioner = nil
    gateway = nil
    node = nil
    EM.run do
      Do.at(0) { provisioner = ProvisionerTests.create_provisioner }
      Do.at(1) { gateway = ProvisionerTests.create_gateway(provisioner) }
      Do.at(2) { node = ProvisionerTests.create_node(1) }
      Do.at(3) { gateway.send_provision_request }
      Do.at(4) { gateway.send_unprovision_request }
      Do.at(5) { EM.stop }
    end
    node.got_unprovision_request.should be_true
  end

  it "should handle error in unprovision" do
    provisioner = nil
    gateway = nil
    node = nil
    EM.run do
      Do.at(0) { provisioner = ProvisionerTests.create_provisioner }
      Do.at(1) { gateway = ProvisionerTests.create_error_gateway(provisioner) }
      Do.at(2) { node = ProvisionerTests.create_error_node(1) }
      Do.at(3) { ProvisionerTests.setup_fake_instance(gateway, provisioner, node) }
      Do.at(4) { gateway.send_unprovision_request }
      Do.at(5) { EM.stop }
    end
    node.got_unprovision_request.should be_true
    gateway.unprovision_response.should be_false
    gateway.error_msg.should_not == nil
    gateway.error_msg['status'].should == 500
    gateway.error_msg['msg']['code'].should == 30500
  end

  it "should support bind" do
    provisioner = nil
    gateway = nil
    node = nil
    EM.run do
      Do.at(0) { provisioner = ProvisionerTests.create_provisioner }
      Do.at(1) { gateway = ProvisionerTests.create_gateway(provisioner) }
      Do.at(2) { node = ProvisionerTests.create_node(1) }
      Do.at(3) { gateway.send_provision_request }
      Do.at(4) { gateway.send_bind_request }
      Do.at(5) { EM.stop }
    end
    gateway.got_provision_response.should be_true
    gateway.got_bind_response.should be_true
  end

  it "should handle error in bind" do
    provisioner = nil
    gateway = nil
    node = nil
    EM.run do
      Do.at(0) { provisioner = ProvisionerTests.create_provisioner }
      Do.at(1) { gateway = ProvisionerTests.create_error_gateway(provisioner) }
      Do.at(2) { node = ProvisionerTests.create_error_node(1) }
      Do.at(3) { ProvisionerTests.setup_fake_instance(gateway, provisioner, node) }
      Do.at(4) { gateway.send_bind_request }
      Do.at(5) { EM.stop }
    end
    node.got_bind_request.should be_true
    gateway.bind_response.should be_false
    gateway.error_msg['status'].should == 500
    gateway.error_msg['msg']['code'].should == 30500
  end

  it "should handle error in unbind" do
    provisioner = nil
    gateway = nil
    node = nil
    EM.run do
      Do.at(0) { provisioner = ProvisionerTests.create_provisioner }
      Do.at(1) { gateway = ProvisionerTests.create_error_gateway(provisioner) }
      Do.at(2) { node = ProvisionerTests.create_error_node(1) }
      Do.at(3) {
        ProvisionerTests.setup_fake_instance(gateway, provisioner, node)
        bind_id = "fake_bind_id"
        gateway.bind_id =  bind_id
        provisioner.prov_svcs[bind_id] = {:credentials => {'node_id' =>node.node_id }}
      }
      Do.at(5) { gateway.send_unbind_request }
      Do.at(6) { EM.stop }
    end
    node.got_unbind_request.should be_true
    gateway.unbind_response.should be_false
    gateway.error_msg['status'].should == 500
    gateway.error_msg['msg']['code'].should == 30500
  end

  it "should support restore" do
    provisioner = nil
    gateway = nil
    node = nil
    EM.run do
      Do.at(0) { provisioner = ProvisionerTests.create_provisioner }
      Do.at(1) { gateway = ProvisionerTests.create_gateway(provisioner) }
      Do.at(2) { node = ProvisionerTests.create_node(1) }
      Do.at(3) { gateway.send_provision_request }
      Do.at(4) { gateway.send_restore_request }
      Do.at(5) { EM.stop }
    end
    gateway.got_restore_response.should be_true
  end

  it "should handle error in restore" do
    provisioner = nil
    gateway = nil
    node = nil
    EM.run do
      Do.at(0) { provisioner = ProvisionerTests.create_provisioner }
      Do.at(1) { gateway = ProvisionerTests.create_error_gateway(provisioner) }
      Do.at(2) { node = ProvisionerTests.create_error_node(1) }
      Do.at(3) { ProvisionerTests.setup_fake_instance(gateway, provisioner, node) }
      Do.at(4) { gateway.send_restore_request }
      Do.at(5) { EM.stop }
    end
    node.got_restore_request.should be_true
    gateway.error_msg['status'].should == 500
    gateway.error_msg['msg']['code'].should == 30500
  end

  it "should support recover" do
    provisioner = nil
    gateway = nil
    node = nil
    EM.run do
      Do.at(0) { provisioner = ProvisionerTests.create_provisioner }
      Do.at(1) { gateway = ProvisionerTests.create_gateway(provisioner) }
      Do.at(2) { node = ProvisionerTests.create_node(1) }
      Do.at(3) { gateway.send_provision_request }
      Do.at(4) { gateway.send_recover_request }
      Do.at(10) { EM.stop }
    end
    gateway.got_recover_response.should be_true
  end

  it "should support varz" do
    provisioner = nil
    gateway = nil
    node = nil
    prov_svcs_before = nil
    prov_svcs_after = nil
    varz_invoked_before = nil
    varz_invoked_after = nil
    EM.run do
      Do.at(0) { provisioner = ProvisionerTests.create_provisioner }
      Do.at(1) { gateway = ProvisionerTests.create_gateway(provisioner) }
      Do.at(2) { node = ProvisionerTests.create_node(1) }
      Do.at(3) { gateway.send_provision_request }
      Do.at(4) { gateway.send_bind_request }
      Do.at(5) {
        prov_svcs_before = Marshal.dump(provisioner.prov_svcs)
        varz_invoked_before = provisioner.varz_invoked
      }
      # varz is invoked 5 seconds after provisioner is created
      Do.at(11) {
        prov_svcs_after = Marshal.dump(provisioner.prov_svcs)
        varz_invoked_after = provisioner.varz_invoked
      }
      Do.at(12) { EM.stop }
    end
    varz_invoked_before.should be_false
    varz_invoked_after.should be_true
    prov_svcs_before.should == prov_svcs_after
  end

  it "should support healthz" do
    provisioner = nil
    gateway = nil
    node = nil
    prov_svcs_before = nil
    prov_svcs_after = nil
    healthz_invoked_before = nil
    healthz_invoked_after = nil
    EM.run do
      Do.at(0) { provisioner = ProvisionerTests.create_provisioner }
      Do.at(1) { gateway = ProvisionerTests.create_gateway(provisioner) }
      Do.at(2) { node = ProvisionerTests.create_node(1) }
      Do.at(3) { gateway.send_provision_request }
      Do.at(4) { gateway.send_bind_request }
      Do.at(5) {
        prov_svcs_before = Marshal.dump(provisioner.prov_svcs)
        healthz_invoked_before = provisioner.healthz_invoked
      }
      # healthz is invoked 5 seconds after provisioner is created
      Do.at(11) {
        prov_svcs_after = Marshal.dump(provisioner.prov_svcs)
        healthz_invoked_after = provisioner.healthz_invoked
      }
      Do.at(12) { EM.stop }
    end
    healthz_invoked_before.should be_false
    healthz_invoked_after.should be_true
    prov_svcs_before.should == prov_svcs_after
  end

  it "should allow over provisioning when it is configured so" do
    provisioner = nil
    gateway = nil
    node = nil
    EM.run do
      Do.at(0) { provisioner = ProvisionerTests.create_provisioner({:allow_over_provisioning => true}) }
      Do.at(1) { gateway = ProvisionerTests.create_gateway(provisioner) }
      Do.at(2) { node = ProvisionerTests.create_node(1, -1) }
      Do.at(3) { gateway.send_provision_request }
      Do.at(4) { EM.stop }
    end
    node.got_provision_request.should be_true
  end

  it "should not allow over provisioning when it is not configured so" do
    provisioner = nil
    gateway = nil
    node = nil
    EM.run do
      Do.at(0) { provisioner = ProvisionerTests.create_provisioner({:allow_over_provisioning => false}) }
      Do.at(1) { gateway = ProvisionerTests.create_gateway(provisioner) }
      Do.at(2) { node = ProvisionerTests.create_node(1, -1) }
      Do.at(3) { gateway.send_provision_request }
      Do.at(4) { EM.stop }
    end
    node.got_provision_request.should be_false
  end

  it "should support check orphan" do
    provisioner = nil
    gateway = nil
    node = nil
    EM.run do
      Do.at(0) { provisioner = ProvisionerTests.create_provisioner }
      Do.at(1) { gateway = ProvisionerTests.create_gateway(provisioner) }
      Do.at(2) { node = ProvisionerTests.create_node(2) }
      Do.at(3) { node = ProvisionerTests.create_node(3) }
      Do.at(4) { gateway.send_check_orphan_request }
      Do.at(8) { gateway.send_double_check_orphan_request }
      Do.at(10) { EM.stop }
    end
    provisioner.staging_orphan_instances["node-2"].count.should == 2
    provisioner.staging_orphan_instances["node-3"].count.should == 2
    provisioner.final_orphan_instances["node-2"].count.should == 1
    provisioner.final_orphan_instances["node-3"].count.should == 2
    provisioner.staging_orphan_bindings["node-2"].count.should == 1
    provisioner.staging_orphan_bindings["node-3"].count.should == 2
    provisioner.final_orphan_bindings["node-2"].count.should == 1
    provisioner.final_orphan_bindings["node-3"].count.should == 2
  end

  it "should handle error in check orphan" do
    provisioner = nil
    gateway = nil
    node = nil
    EM.run do
      Do.at(0) { provisioner = ProvisionerTests.create_provisioner }
      Do.at(1) { gateway = ProvisionerTests.create_error_gateway(provisioner) }
      Do.at(2) { node = ProvisionerTests.create_error_node(1) }
      Do.at(3) { gateway.send_check_orphan_request }
      Do.at(4) { EM.stop }
    end
    node.got_check_orphan_request.should be_true
    provisioner.staging_orphan_instances["node-1"].should be_nil
    provisioner.final_orphan_instances["node-1"].should be_nil
  end

  it "should support purging massive orphans" do
    provisioner = nil
    gateway = nil
    node = nil
    node2 = nil
    EM.run do
      Do.at(0) { provisioner = ProvisionerTests.create_provisioner }
      Do.at(1) { gateway = ProvisionerTests.create_gateway(provisioner, 1024 * 128, 1024 * 16) }
      Do.at(2) { node = ProvisionerTests.create_node(1) }
      Do.at(4) { gateway.send_purge_orphan_request }
      Do.at(60) { EM.stop }
    end
    node.got_purge_orphan_request.should be_true
    gateway.got_purge_orphan_response.should be_true
    node.purge_ins_list.count.should == 1024 * 128
    node.purge_bind_list.count.should == 1024 * 16
  end
end
