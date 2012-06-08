# Copyright (c) 2009-2011 VMware, Inc.
require 'helper/spec_helper'
require 'eventmachine'

describe NodeTests do
  include VCAP::Services::Internal

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

  it "should call varz & report healthz ok" do
    node = nil
    EM.run do
      Do.at(0) { node = NodeTests.create_node }
      Do.at(12) { EM.stop }
    end
    node.varz_invoked.should be_true
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
    mock_nats = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      node = NodeTests.create_node(:plan => "free")
      # assign mock nats to node
      node.nats = mock_nats

      mock_nats.should_receive(:publish).with(any_args())

      req = Yajl::Encoder.encode({"plan" => "free"})
      node.send_node_announcement(req)

      EM.stop
    end
  end

  it "should not announce on different plan" do
    node = nil
    mock_nats = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      node = NodeTests.create_node(:plan => "free")
      # assign mock nats to node
      node.nats = mock_nats

      mock_nats.should_not_receive(:publish).with(any_args())

      req = Yajl::Encoder.encode({"plan" => "nonfree"})
      node.send_node_announcement(req)

      EM.stop
    end
  end

  it "should not announce if not ready" do
    node = nil
    mock_nats = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      node = NodeTests.create_node(:plan => "free")
      # assign mock nats to node
      node.nats = mock_nats
      node.set_ready(false)

      mock_nats.should_not_receive(:publish).with(any_args())

      node.send_node_announcement

      EM.stop
    end
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
    mock_nats = nil
    response = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      node = NodeTests.create_error_node
      # assign mock nats to node
      node.nats = mock_nats

      mock_nats.should_receive(:publish).with(any_args()).and_return { |*args|
          response = VCAP::Services::Internal::SimpleResponse.decode(args[1])
      }

      req = VCAP::Services::Internal::ProvisionRequest.new
      req.plan = "free"
      node.on_provision(req.encode, nil)

      node.provision_invoked.should be_true
      response.success.should be_false
      response.error["status"].should == 503
      response.error["msg"]["code"].should == 30600

      EM.stop
    end
  end

  it "should decrease capacity after successful provision" do
    node = nil
    mock_nats = nil
    original_capacity = 0
    EM.run do
      mock_nats = mock("test_mock_nats")
      node = NodeTests.create_node
      # assign mock nats to node
      node.nats = mock_nats

      mock_nats.should_receive(:publish).with(any_args())

      original_capacity = node.capacity
      req = VCAP::Services::Internal::ProvisionRequest.new
      req.plan = "free"
      node.on_provision(req.encode, nil)

      (original_capacity - node.capacity).should > 0

      EM.stop
    end
  end

  it "should not decrease capacity after erroneous provision" do
    node = nil
    mock_nats = nil
    original_capacity = 0
    EM.run do
      mock_nats = mock("test_mock_nats")
      node = NodeTests.create_error_node
      # assign mock nats to node
      node.nats = mock_nats

      mock_nats.should_receive(:publish).with(any_args())

      original_capacity = node.capacity
      req = VCAP::Services::Internal::ProvisionRequest.new
      req.plan = "free"
      node.on_provision(req.encode, nil)

      (original_capacity - node.capacity).should == 0

      EM.stop
    end
  end

  it "should handle long time provision" do
    node = nil
    provisioner = nil
    EM.run do
      Do.sec(0) do
        node = NodeTests.create_node;
        node.stub!(:provision){ sleep 6; {"name" => "test"} }
        node.should_receive(:provision)
      end
      Do.sec(1) { provisioner = NodeTests.create_error_provisioner}
      Do.sec(2) { provisioner.send_provision_request }
      Do.sec(10) { EM.stop }
    end
    provisioner.response["success"].should be_true
  end

  it "should support unprovision" do
    node = nil
    mock_nats = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      node = NodeTests.create_node
      # assign mock nats to node
      node.nats = mock_nats

      mock_nats.should_receive(:publish).with(any_args())

      req = VCAP::Services::Internal::UnprovisionRequest.new
      req.name = "TestNode"
      req.bindings = [{}]
      node.on_unprovision(req.encode, nil)

      node.unprovision_invoked.should be_true

      EM.stop
    end
  end

  it "should handle error in unprovision" do
    node = nil
    mock_nats = nil
    response = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      node = NodeTests.create_error_node
      # assign mock nats to node
      node.nats = mock_nats

      mock_nats.should_receive(:publish).with(any_args()).and_return { |*args|
          response = VCAP::Services::Internal::SimpleResponse.decode(args[1])
      }

      req = VCAP::Services::Internal::UnprovisionRequest.new
      req.name = "TestNode"
      req.bindings = [{}]
      node.on_unprovision(req.encode, nil)

      node.unprovision_invoked.should be_true
      response.success.should be_false
      response.error["status"].should == 503
      response.error["msg"]["code"].should == 30600

      EM.stop
    end
  end

  it "should increase capacity after successful unprovision" do
    node = nil
    mock_nats = nil
    original_capacity = 0
    EM.run do
      mock_nats = mock("test_mock_nats")
      node = NodeTests.create_node
      # assign mock nats to node
      node.nats = mock_nats

      mock_nats.should_receive(:publish).with(any_args())

      original_capacity = node.capacity
      req = VCAP::Services::Internal::UnprovisionRequest.new
      req.name = "TestNode"
      req.bindings = [{}]
      node.on_unprovision(req.encode, nil)

      (original_capacity - node.capacity).should < 0

      EM.stop
    end
  end

  it "should not increase capacity after erroneous unprovision" do
    node = nil
    mock_nats = nil
    original_capacity = 0
    EM.run do
      mock_nats = mock("test_mock_nats")
      node = NodeTests.create_error_node
      # assign mock nats to node
      node.nats = mock_nats

      mock_nats.should_receive(:publish).with(any_args())

      original_capacity = node.capacity
      req = VCAP::Services::Internal::UnprovisionRequest.new
      req.name = "TestNode"
      req.bindings = [{}]
      node.on_unprovision(req.encode, nil)

      (original_capacity - node.capacity).should == 0

      EM.stop
    end
  end

  it "should support bind" do
    node = nil
    mock_nats = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      node = NodeTests.create_node
      # assign mock nats to node
      node.nats = mock_nats

      mock_nats.should_receive(:publish).with(any_args())

      req = VCAP::Services::Internal::BindRequest.new
      req.name = "fake"
      req.bind_opts = {}
      node.on_bind(req.encode, nil)

      node.bind_invoked.should be_true

      EM.stop
    end
  end

  it "should handle error in bind" do
    node = nil
    mock_nats = nil
    response = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      node = NodeTests.create_error_node
      # assign mock nats to node
      node.nats = mock_nats

      mock_nats.should_receive(:publish).with(any_args()).and_return { |*args|
          response = VCAP::Services::Internal::SimpleResponse.decode(args[1])
      }

      req = VCAP::Services::Internal::BindRequest.new
      req.name = "fake"
      req.bind_opts = {}
      node.on_bind(req.encode, nil)

      node.bind_invoked.should be_true
      response.success.should be_false
      response.error["status"].should == 503
      response.error["msg"]["code"].should == 30600

      EM.stop
    end
  end

  it "should handle long time bind" do
    node = nil
    provisioner = nil
    EM.run do
      Do.sec(0) do
        node = NodeTests.create_node;
        node.stub!(:bind){ sleep 6; BindResponse.new }
        node.should_receive(:bind)
      end
      Do.sec(1) { provisioner = NodeTests.create_error_provisioner}
      Do.sec(2) { provisioner.send_bind_request }
      Do.sec(10) { EM.stop }
    end
    provisioner.response["success"].should be_true
  end


  it "should support unbind" do
    node = nil
    mock_nats = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      node = NodeTests.create_node
      # assign mock nats to node
      node.nats = mock_nats

      mock_nats.should_receive(:publish).with(any_args())

      req = VCAP::Services::Internal::UnbindRequest.new
      req.credentials = {}
      node.on_unbind(req.encode, nil)

      node.unbind_invoked.should be_true

      EM.stop
    end
  end

  it "should handle error in unbind" do
    node = nil
    mock_nats = nil
    response = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      node = NodeTests.create_error_node
      # assign mock nats to node
      node.nats = mock_nats

      mock_nats.should_receive(:publish).with(any_args()).and_return { |*args|
          response = VCAP::Services::Internal::SimpleResponse.decode(args[1])
      }

      req = VCAP::Services::Internal::UnbindRequest.new
      req.credentials = {}
      node.on_unbind(req.encode, nil)

      node.unbind_invoked.should be_true
      response.success.should be_false
      response.error["status"].should == 503
      response.error["msg"]["code"].should == 30600

      EM.stop
    end
  end

  it "should support restore" do
    node = nil
    mock_nats = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      node = NodeTests.create_node
      # assign mock nats to node
      node.nats = mock_nats

      mock_nats.should_receive(:publish).with(any_args())

      req = VCAP::Services::Internal::RestoreRequest.new
      req.instance_id = "fake1"
      req.backup_path = "/tmp"
      node.on_restore(req.encode, nil)

      node.restore_invoked.should be_true

      EM.stop
    end
  end

  it "should handle error in restore" do
    node = nil
    mock_nats = nil
    response = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      node = NodeTests.create_error_node
      # assign mock nats to node
      node.nats = mock_nats

      mock_nats.should_receive(:publish).with(any_args()).and_return { |*args|
          response = VCAP::Services::Internal::SimpleResponse.decode(args[1])
      }

      req = VCAP::Services::Internal::RestoreRequest.new
      req.instance_id = "fake1"
      req.backup_path = "/tmp"
      node.on_restore(req.encode, nil)

      node.restore_invoked.should be_true
      response.success.should be_false
      response.error["status"].should == 503
      response.error["msg"]["code"].should == 30600

      EM.stop
    end
  end

  it "should support disable instance" do
    node = nil
    mock_nats = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      node = NodeTests.create_node
      # assign mock nats to node
      node.nats = mock_nats

      mock_nats.should_receive(:publish).with(any_args())

      req = [{"service_id" => "fake1", "configuration" => {"plan" => "free"},\
              "credentials" => {"name" => "fake1"}}, []]
      node.on_disable_instance(Yajl::Encoder.encode(req), nil)

      node.disable_invoked.should be_true

      EM.stop
    end
  end

  it "should handle error in disable instance" do
    node = nil
    mock_nats = nil
    response = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      node = NodeTests.create_error_node
      # assign mock nats to node
      node.nats = mock_nats

      mock_nats.should_receive(:publish).with(any_args()).and_return { |*args|
          response = VCAP::Services::Internal::SimpleResponse.decode(args[1])
      }

      req = [{"service_id" => "fake1", "configuration" => {"plan" => "free"},\
              "credentials" => {"name" => "fake1"}}, []]
      node.on_disable_instance(Yajl::Encoder.encode(req), nil)

      node.disable_invoked.should be_true
      response.success.should be_false
      response.error["status"].should == 503
      response.error["msg"]["code"].should == 30600

      EM.stop
    end
  end

  it "should support enable instance" do
    node = nil
    mock_nats = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      node = NodeTests.create_node
      # assign mock nats to node
      node.nats = mock_nats

      mock_nats.should_receive(:publish).with(any_args())

      req = [{"service_id" => "fake1", "configuration" => {"plan" => "free"},\
              "credentials" => {"name" => "fake1"}}, []]
      node.on_enable_instance(Yajl::Encoder.encode(req), nil)

      node.enable_invoked.should be_true

      EM.stop
    end
  end

  it "should handle error in enable instance" do
    node = nil
    mock_nats = nil
    response = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      node = NodeTests.create_error_node
      # assign mock nats to node
      node.nats = mock_nats

      mock_nats.should_receive(:publish).with(any_args()).and_return { |*args|
          response = VCAP::Services::Internal::SimpleResponse.decode(args[1])
      }

      req = [{"service_id" => "fake1", "configuration" => {"plan" => "free"},\
              "credentials" => {"name" => "fake1"}}, []]
      node.on_enable_instance(Yajl::Encoder.encode(req), nil)

      node.enable_invoked.should be_true
      response.success.should be_false
      response.error["status"].should == 503
      response.error["msg"]["code"].should == 30600

      EM.stop
    end
  end

  it "should support import instance" do
    node = nil
    mock_nats = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      node = NodeTests.create_node
      # assign mock nats to node
      node.nats = mock_nats

      mock_nats.should_receive(:publish).with(any_args())

      req = [{"service_id" => "fake1", "configuration" => {"plan" => "free"},\
              "credentials" => {"name" => "fake1"}}, []]
      node.on_import_instance(Yajl::Encoder.encode(req), nil)

      node.import_invoked.should be_true

      EM.stop
    end
  end

  it "should handle error in import instance" do
    node = nil
    mock_nats = nil
    response = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      node = NodeTests.create_error_node
      # assign mock nats to node
      node.nats = mock_nats

      mock_nats.should_receive(:publish).with(any_args()).and_return { |*args|
          response = VCAP::Services::Internal::SimpleResponse.decode(args[1])
      }

      req = [{"service_id" => "fake1", "configuration" => {"plan" => "free"},\
              "credentials" => {"name" => "fake1"}}, []]
      node.on_import_instance(Yajl::Encoder.encode(req), nil)

      node.import_invoked.should be_true
      response.success.should be_false
      response.error["status"].should == 503
      response.error["msg"]["code"].should == 30600

      EM.stop
    end
  end

  it "should support update instance" do
    node = nil
    mock_nats = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      node = NodeTests.create_node
      # assign mock nats to node
      node.nats = mock_nats

      mock_nats.should_receive(:publish).with(any_args())

      req = [{"service_id" => "fake1", "configuration" => {"plan" => "free"},\
              "credentials" => {"name" => "fake1"}}, []]
      node.on_update_instance(Yajl::Encoder.encode(req), nil)

      node.update_invoked.should be_true

      EM.stop
    end
  end

  it "should handle error in update instance" do
    node = nil
    mock_nats = nil
    response = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      node = NodeTests.create_error_node
      # assign mock nats to node
      node.nats = mock_nats

      mock_nats.should_receive(:publish).with(any_args()).and_return { |*args|
          response = VCAP::Services::Internal::SimpleResponse.decode(args[1])
      }

      req = [{"service_id" => "fake1", "configuration" => {"plan" => "free"},\
              "credentials" => {"name" => "fake1"}}, []]
      node.on_update_instance(Yajl::Encoder.encode(req), nil)

      node.update_invoked.should be_true
      response.success.should be_false
      response.error["status"].should == 503
      response.error["msg"]["code"].should == 30600

      EM.stop
    end
  end

  it "should support cleanupnfs instance" do
    node = nil
    mock_nats = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      node = NodeTests.create_node
      # assign mock nats to node
      node.nats = mock_nats

      FileUtils.mkdir_p("/tmp/migration/Test/fake1")

      mock_nats.should_receive(:publish).with(any_args())

      req = [{"service_id" => "fake1", "configuration" => {"plan" => "free"},\
              "credentials" => {"name" => "fake1"}}, []]
      node.on_cleanupnfs_instance(Yajl::Encoder.encode(req), nil)

      File.exists?("/tmp/migration/Test/fake1").should be_false

      EM.stop
    end
  end

  it "should decrease capacity after successful migration" do
    node = nil
    mock_nats = nil
    original_capacity = 0
    EM.run do
      mock_nats = mock("test_mock_nats")
      node = NodeTests.create_node
      # assign mock nats to node
      node.nats = mock_nats

      mock_nats.should_receive(:publish).with(any_args)

      original_capacity = node.capacity
      req = [{"service_id" => "fake1", "configuration" => {"plan" => "free"},\
              "credentials" => {"name" => "fake1"}}, []]
      node.on_update_instance(Yajl::Encoder.encode(req), nil)

      (original_capacity - node.capacity).should == 1

      EM.stop
    end
  end

  it "should not decrease capacity after erroneous migration" do
    node = nil
    mock_nats = nil
    original_capacity = 0
    EM.run do
      mock_nats = mock("test_mock_nats")
      node = NodeTests.create_error_node
      # assign mock nats to node
      node.nats = mock_nats

      mock_nats.should_receive(:publish).with(any_args())

      original_capacity = node.capacity
      req = [{"service_id" => "fake1", "configuration" => {"plan" => "free"},\
              "credentials" => {"name" => "fake1"}}, []]
      node.on_update_instance(Yajl::Encoder.encode(req), nil)

      (original_capacity - node.capacity).should == 0

      EM.stop
    end
  end

  it "should support check_orphan when no handles" do
    node = nil
    mock_nats = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      node = NodeTests.create_node
      # assign mock nats to node
      node.nats = mock_nats

      mock_nats.should_not_receive(:publish).with(any_args())

      node.on_check_orphan(nil, nil)

      EM.stop
    end
  end

  it "should support check_orphan when node has massive \
      instances/bindings/handles" do
    node = nil
    provisioner = nil
    mock_nats = nil
    ins_hash = []
    bind_hash = []
    EM.run do
      mock_nats = mock("test_mock_nats")
      node = NodeTests.create_node(:ins_count => 1024 * 128,
                                   :bind_count => 1024 * 16)
      # assign mock nats to node
      node.nats = mock_nats

      mock_nats.should_receive(:publish).at_least(:once).with(any_args()).\
      and_return { |*args|
          req = VCAP::Services::Internal::NodeHandlesReport.decode(args[1])
          ins_hash.concat(req.instances_list)
          bind_hash.concat(req.bindings_list)
      }
      # mock nats subscribe callback function only can be invoked manually
      node.on_check_orphan(nil, nil)

      ins_hash.count.should == 1024 * 128
      bind_hash.count.should == 1024 * 16

      EM.stop
    end
  end

  it "should support purge_orphan" do
    node = nil
    mock_nats = mock("test_mock_nats")
    EM.run do
      node = NodeTests.create_node
      # assign mock nats to provisioner
      node.nats = mock_nats

      # mock nats subscribe callback function only can be invoked manually
      req = VCAP::Services::Internal::PurgeOrphanRequest.new
      req.orphan_ins_list = TEST_PURGE_INS_HASH[TEST_NODE_ID]
      req.orphan_binding_list = TEST_PURGE_BIND_HASH[TEST_NODE_ID]
      node.on_purge_orphan(req.encode, nil)

      node.unprovision_count.should == 2
      node.unbind_count.should == 2

      EM.stop
    end
  end
end
