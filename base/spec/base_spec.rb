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

end

describe BaseTests do

  it "should connect to node message bus" do
    base = nil
    EM.run do
      NATS.start(:uri => BaseTests::Options::NATS_URI, :autostart => true) {
        Do.at(0) { base = BaseTests.create_base }
        Do.at(1) { EM.stop ; NATS.stop }
      }
    end
    base.node_mbus_connected.should be_true
  end

end

describe NodeTests do

  it "should announce on startup" do
    node = nil
    provisioner = nil
    EM.run do
      NATS.start(:uri => BaseTests::Options::NATS_URI, :autostart => true) {
        # start provisioner then node
        Do.at(0) { provisioner = NodeTests.create_provisioner }
        Do.at(1) { node = NodeTests.create_node }
        Do.at(2) { EM.stop ; NATS.stop }
      }
    end
    provisioner.got_announcement.should be_true
  end

  it "should anounce on request" do
    node = nil
    provisioner = nil
    EM.run do
      NATS.start(:uri => BaseTests::Options::NATS_URI, :autostart => true) {
        # start node then provisioner
        Do.at(0) { node = NodeTests.create_node }
        Do.at(1) { provisioner = NodeTests.create_provisioner }
        Do.at(2) { EM.stop ; NATS.stop }
      }
    end
    node.announcement_invoked.should be_true
    provisioner.got_announcement.should be_true
  end
  it "should support provision" do
    node = nil
    provisioner = nil
    EM.run do
      NATS.start(:uri => BaseTests::Options::NATS_URI, :autostart => true) {
        # start node then provisioner
        Do.at(0) { node = NodeTests.create_node }
        Do.at(1) { provisioner = NodeTests.create_provisioner }
        Do.at(2) { provisioner.send_provision_request }
        Do.at(3) { EM.stop ; NATS.stop }

      }
    end
    node.provision_invoked.should be_true
    provisioner.got_provision_response.should be_true
  end

  it "should support unprovision" do
    node = nil
    provisioner = nil
    EM.run do
      NATS.start(:uri => BaseTests::Options::NATS_URI, :autostart => true) {
        # start node then provisioner
        Do.at(0) { node = NodeTests.create_node }
        Do.at(1) { provisioner = NodeTests.create_provisioner }
        Do.at(2) { provisioner.send_unprovision_request }
        Do.at(3) { EM.stop ; NATS.stop }
      }
    end
    node.unprovision_invoked.should be_true
  end

end

describe ProvisionerTests do

  it "should autodiscover 1 node when started first" do
    provisioner = nil
    node = nil
    # start provisioner, then node
    EM.run do
      NATS.start(:uri => BaseTests::Options::NATS_URI, :autostart => true) {
        Do.at(0) { provisioner = ProvisionerTests.create_provisioner }
        Do.at(1) { node = ProvisionerTests.create_node(1) }
        Do.at(2) { EM.stop ; NATS.stop }
      }
    end
    provisioner.node_count.should == 1
  end

  it "should autodiscover 1 node when started second" do
    provisioner = nil
    node = nil
    EM.run do
      NATS.start(:uri => BaseTests::Options::NATS_URI, :autostart => true) {
        # start node, then provisioner
        Do.at(0) { node = ProvisionerTests.create_node(1) }
        Do.at(1) { provisioner = ProvisionerTests.create_provisioner }
        Do.at(2) { EM.stop ; NATS.stop }
      }
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
      NATS.start(:uri => BaseTests::Options::NATS_URI, :autostart => true) {
        Do.at(0) { provisioner = ProvisionerTests.create_provisioner }
        Do.at(1) { node1 = ProvisionerTests.create_node(1) }
        Do.at(2) { node2 = ProvisionerTests.create_node(2) }
        Do.at(3) { node3 = ProvisionerTests.create_node(3) }
        Do.at(4) { EM.stop ; NATS.stop }
      }
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
      NATS.start(:uri => BaseTests::Options::NATS_URI, :autostart => true) {
        Do.at(0) { node1 = ProvisionerTests.create_node(1) }
        Do.at(1) { node2 = ProvisionerTests.create_node(2) }
        Do.at(2) { node3 = ProvisionerTests.create_node(3) }
        Do.at(3) { provisioner = ProvisionerTests.create_provisioner }
        Do.at(4) { EM.stop ; NATS.stop }
      }
    end
    provisioner.node_count.should == 3
  end

  it "should support provision" do
    provisioner = nil
    gateway = nil
    node = nil
    EM.run do
      NATS.start(:uri => BaseTests::Options::NATS_URI, :autostart => true) {
        Do.at(0) { provisioner = ProvisionerTests.create_provisioner }
        Do.at(1) { gateway = ProvisionerTests.create_gateway(provisioner) }
        Do.at(2) { node = ProvisionerTests.create_node(1) }
        Do.at(3) { gateway.send_provision_request }
        Do.at(4) { EM.stop ; NATS.stop }
      }
    end
    gateway.got_provision_response.should be_true
  end

  it "should pick the best node when provisioning" do
    provisioner = nil
    gateway = nil
    node1 = nil
    node2 = nil
    EM.run do
      NATS.start(:uri => BaseTests::Options::NATS_URI, :autostart => true) {
        Do.at(0) { provisioner = ProvisionerTests.create_provisioner }
        Do.at(1) { gateway = ProvisionerTests.create_gateway(provisioner) }
        Do.at(2) { node1 = ProvisionerTests.create_node(1) }
        Do.at(3) { node2 = ProvisionerTests.create_node(2) }
        Do.at(4) { gateway.send_provision_request }
        Do.at(5) { EM.stop ; NATS.stop }
      }
    end
    node1.got_provision_request.should be_false
    node2.got_provision_request.should be_true
  end

  it "should support unprovision" do
    provisioner = nil
    gateway = nil
    node = nil
    EM.run do
      NATS.start(:uri => BaseTests::Options::NATS_URI, :autostart => true) {
        Do.at(0) { provisioner = ProvisionerTests.create_provisioner }
        Do.at(1) { gateway = ProvisionerTests.create_gateway(provisioner) }
        Do.at(2) { node = ProvisionerTests.create_node(1) }
        Do.at(3) { gateway.send_provision_request }
        Do.at(4) { gateway.send_unprovision_request }
        Do.at(5) { EM.stop ; NATS.stop }
      }
    end
    node.got_unprovision_request.should be_true
  end

end
