# Copyright (c) 2009-2011 VMware, Inc.
require 'helper/spec_helper'
require 'eventmachine'

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

  it "should avoid over provision when provisioning " do
    provisioner = nil
    gateway = nil
    node1 = nil
    node2 = nil
    EM.run do
      Do.at(0) { provisioner = ProvisionerTests.create_provisioner }
      Do.at(1) { gateway = ProvisionerTests.create_gateway(provisioner) }
      Do.at(2) { node1 = ProvisionerTests.create_node(1, 1) }
      Do.at(3) { node2 = ProvisionerTests.create_node(2, 1) }
      Do.at(4) { gateway.send_provision_request; gateway.send_provision_request }
      Do.at(10) { gateway.send_provision_request }
      Do.at(15) { EM.stop }
    end
    node1.got_provision_request.should be_true
    node2.got_provision_request.should be_true
  end

  it "should raise error on provisioning error plan" do
    provisioner = nil
    gateway = nil
    node = nil
    EM.run do
      Do.at(0) { provisioner = ProvisionerTests.create_provisioner }
      Do.at(1) { gateway = ProvisionerTests.create_error_gateway(provisioner) }
      Do.at(2) { node = ProvisionerTests.create_node(1) }
      Do.at(3) { gateway.send_provision_request("error_plan") }
      Do.at(4) { EM.stop }
    end
    node.got_provision_request.should be_false
    gateway.provision_response.should be_false
    gateway.error_msg['status'].should == 400
    gateway.error_msg['msg']['code'].should == 30003
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

  it "should delete instance handles in cache after unprovision" do
    provisioner = gateway = nil
    node = nil
    EM.run do
      Do.at(0) { provisioner = ProvisionerTests.create_provisioner; provisioner.prov_svcs.size.should == 0 }
      Do.at(1) { gateway = ProvisionerTests.create_gateway(provisioner) }
      Do.at(2) { node = ProvisionerTests.create_node(1) }
      Do.at(3) { gateway.send_provision_request }
      Do.at(4) { gateway.send_bind_request }
      Do.at(5) { gateway.send_unprovision_request }
      Do.at(6) { EM.stop }
    end
    node.got_provision_request.should be_true
    node.got_bind_request.should be_true
    node.got_unprovision_request.should be_true
    current_cache = provisioner.prov_svcs
    current_cache.size.should == 0
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
      Do.at(2) { node = ProvisionerTests.create_node(1, 2) }
      Do.at(3) { gateway.send_provision_request }
      Do.at(4) { gateway.send_recover_request }
      Do.at(10) { EM.stop }
    end
    gateway.got_recover_response.should be_true
  end

  it "should support migration" do
    provisioner = nil
    gateway = nil
    node = nil
    EM.run do
      Do.at(0) { provisioner = ProvisionerTests.create_provisioner }
      Do.at(1) { gateway = ProvisionerTests.create_gateway(provisioner) }
      Do.at(2) { node = ProvisionerTests.create_node(1, 2) }
      Do.at(3) { gateway.send_provision_request }
      Do.at(4) { gateway.send_migrate_request("node-1") }
      Do.at(10) { EM.stop }
    end
    gateway.got_migrate_response.should be_true
  end

  it "should handle error in migration" do
    provisioner = nil
    gateway = nil
    node = nil
    EM.run do
      Do.at(0) { provisioner = ProvisionerTests.create_provisioner }
      Do.at(1) { gateway = ProvisionerTests.create_error_gateway(provisioner) }
      Do.at(2) { node = ProvisionerTests.create_error_node(1) }
      Do.at(3) { ProvisionerTests.setup_fake_instance(gateway, provisioner, node) }
      Do.at(4) { gateway.send_migrate_request("node-1") }
      Do.at(5) { EM.stop }
    end
    node.got_migrate_request.should be_true
    gateway.error_msg['status'].should == 500
    gateway.error_msg['msg']['code'].should == 30500
  end

  it "should support get instance id list" do
    provisioner = nil
    gateway = nil
    node = nil
    EM.run do
      Do.at(0) { provisioner = ProvisionerTests.create_provisioner }
      Do.at(1) { gateway = ProvisionerTests.create_gateway(provisioner) }
      Do.at(2) { node = ProvisionerTests.create_node(1) }
      Do.at(3) { gateway.send_provision_request }
      Do.at(4) { gateway.send_instances_request("node-1") }
      Do.at(5) { EM.stop }
    end
    gateway.got_instances_response.should be_true
  end

  it "should handle error in getting instance id list" do
    provisioner = nil
    gateway = nil
    node = nil
    EM.run do
      Do.at(0) { provisioner = ProvisionerTests.create_provisioner }
      Do.at(1) { gateway = ProvisionerTests.create_error_gateway(provisioner) }
      Do.at(2) { node = ProvisionerTests.create_error_node(1) }
      Do.at(3) { ProvisionerTests.setup_fake_instance(gateway, provisioner, node) }
      Do.at(4) { gateway.send_migrate_request("node-1") }
      Do.at(5) { EM.stop }
    end
    gateway.error_msg['status'].should == 500
    gateway.error_msg['msg']['code'].should == 30500
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

  it "should allow over provisioning when it is configured so" do
    provisioner = nil
    gateway = nil
    node = nil
    EM.run do
      Do.at(0) do
        provisioner = ProvisionerTests.create_provisioner({
          :plan_management => {
          :plans => {
          :free => {
          :allow_over_provisioning => true
        } } } })
      end
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
      Do.at(0) do
        provisioner = ProvisionerTests.create_provisioner({
          :plan_management => {
          :plans => {
          :free => {
          :allow_over_provisioning => false
        } } } })
      end
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
