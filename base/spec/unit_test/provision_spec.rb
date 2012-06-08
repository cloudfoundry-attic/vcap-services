# Copyright (c) 2009-2011 VMware, Inc.
require 'helper/spec_helper'
require 'eventmachine'
require 'base/service_message'

include VCAP::Services::Base::Error

describe ProvisionerTests do
  it "should autodiscover 1 node when started first" do
    provisioner = nil
    mock_nats = nil
    # start provisioner only, send fake node announce message
    EM.run do
      mock_nats = mock("test_mock_nats")
      provisioner = ProvisionerTests.create_provisioner
      # assign mock nats to provisioner
      provisioner.nats = mock_nats

      msg = {}
      msg[:id] = "node-1"
      msg[:plan] = "free"
      msg[:available_capacity] = 200

      # mock nats subscribe callback function only can be invoked manually
      provisioner.on_announce(Yajl::Encoder.encode(msg))

      provisioner.node_count.should == 1

      EM.stop
    end
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
    mock_nats = nil
    node1 = nil
    node2 = nil
    node3 = nil
    # start provisioner only, send fake node announce message
    EM.run do
      mock_nats = mock("test_mock_nats")
      provisioner = ProvisionerTests.create_provisioner
      # assign mock nats to provisioner
      provisioner.nats = mock_nats

      node1 = {}
      node1[:id] = "node-1"
      node1[:plan] = "free"
      node1[:available_capacity] = 200
      node2 = {}
      node2[:id] = "node-2"
      node2[:plan] = "free"
      node2[:available_capacity] = 200
      node3 = {}
      node3[:id] = " node-3"
      node3[:plan] = "free"
      node3[:available_capacity] = 200

      # mock nats subscribe callback function only can be invoked manually
      provisioner.on_announce(Yajl::Encoder.encode(node1))
      provisioner.on_announce(Yajl::Encoder.encode(node2))
      provisioner.on_announce(Yajl::Encoder.encode(node3))

      provisioner.node_count.should == 3

      EM.stop
    end
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
    mock_nats = nil
    mock_nodes = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      provisioner = ProvisionerTests.create_provisioner
      # assign mock nats to provisioner
      provisioner.nats = mock_nats
      gateway = ProvisionerTests.create_gateway(provisioner)
      # provisioner pursues best node to send provision request
      mock_nodes = {
          "node-1" => {
              "id" => "node-1",
              "plan" => "free",
              "available_capacity" => 200,
              "capacity_unit" => 1,
              "time" => Time.now.to_i
          }
      }
      provisioner.nodes = mock_nodes

      mock_nats.should_receive(:request).with(any_args()).and_return { |*args|
          callback = args[2]
          response = VCAP::Services::Internal::ProvisionResponse.new
          response.success = true
          response.credentials = {
              "node_id" => "node-1",
              "name" => "D501B915-5B50-4C3A-93B7-7E0C48B6A9FA"
          }
          callback.call(response.encode)
          "5"
      }
      mock_nats.should_receive(:unsubscribe).with(any_args())

      gateway.send_provision_request

      gateway.got_provision_response.should be_true

      EM.stop
    end
  end

  it "should handle error in provision" do
    provisioner = nil
    gateway = nil
    mock_nats = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      provisioner = ProvisionerTests.create_provisioner
      # assign mock nats to provisioner
      provisioner.nats = mock_nats
      gateway = ProvisionerTests.create_error_gateway(provisioner)
      # provisioner pursues best node to send provision request
      mock_nodes = {
          "node-error-1" => {
              "id" => "node-error-1",
              "plan" => "free",
              "available_capacity" => 200,
              "capacity_unit" => 1,
              "time" => Time.now.to_i
          }
      }
      provisioner.nodes = mock_nodes

      mock_nats.should_receive(:request).with(any_args()).and_return { |*args|
          callback = args[2]
          response = VCAP::Services::Internal::ProvisionResponse.new
          response.success = false
          response.error = ServiceError.new(ServiceError::INTERNAL_ERROR).\
                                        to_hash
          callback.call(response.encode)
          "5"
      }
      mock_nats.should_receive(:unsubscribe).with(any_args())

      gateway.send_provision_request

      gateway.provision_response.should be_false
      gateway.error_msg['status'].should == 500
      gateway.error_msg['msg']['code'].should == 30500

      EM.stop
    end
  end

  it "should pick the best node when provisioning" do
    provisioner = nil
    gateway = nil
    mock_nats = nil
    nats_request = ""
    EM.run do
      mock_nats = mock("test_mock_nats")
      provisioner = ProvisionerTests.create_provisioner
      # assign mock nats to provisioner
      provisioner.nats = mock_nats
      gateway = ProvisionerTests.create_gateway(provisioner)
      # provisioner picks up best node from all nodes to send provision request
      mock_nodes = {
          "node-1" => {
              "id" => "node-1",
              "plan" => "free",
              "available_capacity" => 200,
              "capacity_unit" => 1,
              "time" => Time.now.to_i
          },
          "node-2" => {
              "id" => "node-2",
              "plan" => "free",
              "available_capacity" => 100,
              "capacity_unit" => 1,
              "time" => Time.now.to_i

          }
      }
      provisioner.nodes = mock_nodes

      mock_nats.should_receive(:request).with(any_args()).and_return { |*args|
          nats_request = args[0]
          "5"
      }

      gateway.send_provision_request

      nats_request.should == "Test.provision.node-1"

      EM.stop
    end
  end

  it "should avoid over provision when provisioning " do
    provisioner = nil
    gateway = nil
    mock_nats = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      provisioner = ProvisionerTests.create_provisioner
      # assign mock nats to provisioner
      provisioner.nats = mock_nats
      gateway = ProvisionerTests.create_gateway(provisioner)
      # provisioner pursues best node to send provision request
      mock_nodes = {
          "node-1" => {
              "id" => "node-1",
              "plan" => "free",
              "available_capacity" => 1,
              "capacity_unit" => 1,
              "time" => Time.now.to_i
          }
      }
      provisioner.nodes = mock_nodes

      mock_nats.should_receive(:request).with(any_args()).and_return { |*args|
          callback = args[2]
          response = VCAP::Services::Internal::ProvisionResponse.new
          response.success = true
          response.credentials = {
              "node_id" => "node-1",
              "name" => "622b4424-a644-4fcc-a363-6acb5f4952dd"
          }
          callback.call(response.encode)
          "5"
      }
      mock_nats.should_receive(:unsubscribe).with(any_args())

      gateway.send_provision_request # first request get success
      gateway.send_provision_request # second request get error

      gateway.got_provision_response.should be_false

      EM.stop
    end
  end

  it "should raise error on provisioning error plan" do
    provisioner = nil
    gateway = nil
    mock_nats = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      provisioner = ProvisionerTests.create_provisioner
      # assign mock nats to provisioner
      provisioner.nats = mock_nats
      gateway = ProvisionerTests.create_error_gateway(provisioner)
      # provisioner pursues best node to send provision request
      mock_nodes = {
          "node-1" => {
              "id" => "node-1",
              "plan" => "free",
              "available_capacity" => 200,
              "capacity_unit" => 1,
              "time" => Time.now.to_i
          }
      }
      provisioner.nodes = mock_nodes

      mock_nats.should_not_receive(:request).with(any_args())

      gateway.send_provision_request("error_plan")

      gateway.provision_response.should be_false
      gateway.error_msg['status'].should == 400
      gateway.error_msg['msg']['code'].should == 30003

      EM.stop
    end
  end

  it "should support unprovision" do
    provisioner = nil
    gateway = nil
    mock_nats = nil
    provision_request = ""
    EM.run do
      mock_nats = mock("test_mock_nats")
      provisioner = ProvisionerTests.create_provisioner
      # assign mock nats to provisioner
      provisioner.nats = mock_nats
      gateway = ProvisionerTests.create_gateway(provisioner)
      # mock node to send provision & unprovision request
      mock_nodes = {
          "node-1" => {
              "id" => "node-1",
              "plan" => "free",
              "available_capacity" => 200,
              "capacity_unit" => 1,
              "time" => Time.now.to_i
          }
      }
      provisioner.nodes = mock_nodes

      mock_nats.should_receive(:request).twice.with(any_args()).\
      and_return { |*args|
          provision_request = args[0]
          if provision_request == "Test.provision.node-1"
            callback = args[2]
            response = VCAP::Services::Internal::ProvisionResponse.new
            response.success = true
            response.credentials = {
                "node_id" => "node-1",
                "name" => "622b4424-a644-4fcc-a363-6acb5f4952dd"
            }
            callback.call(response.encode)
          else
            callback = args[2]
            response = VCAP::Services::Internal::SimpleResponse.new
            response.success = true
            callback.call(response.encode)
          end
          "5"
      }
      mock_nats.should_receive(:unsubscribe).twice.with(any_args())

      gateway.send_provision_request
      provision_request.should == "Test.provision.node-1"

      gateway.send_unprovision_request
      provision_request.should == "Test.unprovision.node-1"

      EM.stop
    end
  end

  it "should delete instance handles in cache after unprovision" do
    provisioner = gateway = nil
    mock_nats = nil
    provision_request = ""
    EM.run do
      mock_nats = mock("test_mock_nats")
      provisioner = ProvisionerTests.create_provisioner
      provisioner.prov_svcs.size.should == 0
      # assign mock nats to provisioner
      provisioner.nats = mock_nats
      gateway = ProvisionerTests.create_gateway(provisioner)
      # mock node to send unprovision request
      mock_nodes = {
          "node-1" => {
              "id" => "node-1",
              "plan" => "free",
              "available_capacity" => 200,
              "capacity_unit" => 1,
              "time" => Time.now.to_i
          }
      }
      provisioner.nodes = mock_nodes

      mock_nats.should_receive(:request).exactly(3).times.with(any_args()).\
      and_return { |*args|
          provision_request = args[0]
          if provision_request == "Test.provision.node-1"
            callback = args[2]
            response = VCAP::Services::Internal::ProvisionResponse.new
            response.success = true
            response.credentials = {
                "node_id" => "node-1",
                "name" => "622b4424-a644-4fcc-a363-6acb5f4952dd"
            }
            callback.call(response.encode)
          elsif provision_request == "Test.unprovision.node-1"
            callback = args[2]
            response = VCAP::Services::Internal::SimpleResponse.new
            response.success = true
            callback.call(response.encode)
          else
            callback = args[2]
            response = VCAP::Services::Internal::BindResponse.new
            response.success = true
            response.credentials = {
                "name" => "622b4424-a644-4fcc-a363-6acb5f4952dd"
            }
            callback.call(response.encode)
          end
          "5"
      }
      mock_nats.should_receive(:unsubscribe).exactly(3).times.with(any_args())

      gateway.send_provision_request
      provision_request.should == "Test.provision.node-1"

      gateway.send_bind_request
      provision_request.should == "Test.bind.node-1"

      gateway.send_unprovision_request
      provision_request.should == "Test.unprovision.node-1"

      current_cache = provisioner.prov_svcs
      current_cache.size.should == 0

      EM.stop
    end
  end

  it "should handle error in unprovision" do
    provisioner = nil
    gateway = nil
    mock_nats = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      provisioner = ProvisionerTests.create_provisioner
      # assign mock nats to provisioner
      provisioner.nats = mock_nats
      gateway = ProvisionerTests.create_error_gateway(provisioner)
      # mock node to send unprovision request
      mock_nodes = {
          "node-1" => {
              "id" => "node-1",
              "plan" => "free",
              "available_capacity" => 200,
              "capacity_unit" => 1,
              "time" => Time.now.to_i
          }
      }
      provisioner.nodes = mock_nodes

      mock_nats.should_receive(:request).with(any_args()).and_return { |*args|
          callback = args[2]
          response = VCAP::Services::Internal::SimpleResponse.new
          response.success = false
          response.error = ServiceError.new(ServiceError::INTERNAL_ERROR).\
                                        to_hash
          callback.call(response.encode)
          "5"
      }
      mock_nats.should_receive(:unsubscribe).with(any_args())

      ProvisionerTests.setup_fake_instance_by_id(gateway, provisioner, "node-1")

      gateway.send_unprovision_request

      gateway.unprovision_response.should be_false
      gateway.error_msg['status'].should == 500
      gateway.error_msg['msg']['code'].should == 30500

      EM.stop
    end
  end

  it "should support bind" do
    provisioner = nil
    gateway = nil
    mock_nats = nil
    provision_request = ""
    EM.run do
      mock_nats = mock("test_mock_nats")
      provisioner = ProvisionerTests.create_provisioner
      # assgin mock nats to provisioner
      provisioner.nats = mock_nats
      gateway = ProvisionerTests.create_gateway(provisioner)
      # mock node to send provision & bind request
      mock_nodes = {
          "node-1" => {
              "id" => "node-1",
              "plan" => "free",
              "available_capacity" => 200,
              "capacity_unit" => 1,
              "time" => Time.now.to_i
          }
      }
      provisioner.nodes = mock_nodes

      mock_nats.should_receive(:request).twice.with(any_args()).\
      and_return { |*args|
          provision_request = args[0]
          callback = args[2]
          if provision_request == "Test.provision.node-1"
            response = VCAP::Services::Internal::ProvisionResponse.new
            response.success = true
            response.credentials = {
                "node_id" => "node-1",
                "name" => "b66e62e8-c87a-4adf-b08b-3cd30fcdbebb"
            }
            callback.call(response.encode)
          else
            response = VCAP::Services::Internal::BindResponse.new
            response.success = true
            response.credentials = {
                "node_id" => "node-1",
                "name" => "b66e62e8-c87a-4adf-b08b-3cd30fcdbebb"
            }
            callback.call(response.encode)
          end
          "5"
      }
      mock_nats.should_receive(:unsubscribe).twice.with(any_args())

      gateway.send_provision_request
      gateway.got_provision_response.should be_true

      gateway.send_bind_request
      gateway.got_bind_response.should be_true

      EM.stop
    end
  end

  it "should handle error in bind" do
    provisioner = nil
    gateway = nil
    mock_nats = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      provisioner = ProvisionerTests.create_provisioner
      # assign mock nats to provisioner
      provisioner.nats = mock_nats
      gateway = ProvisionerTests.create_error_gateway(provisioner)
      # mock node to send bind request
      mock_nodes = {
          "node-1" => {
              "id" => "node-1",
              "plan" => "free",
              "available_capacity" => 200,
              "capacity_unit" => 1,
              "time" => Time.now.to_i
          }
      }
      provisioner.nodes = mock_nodes

      mock_nats.should_receive(:request).with(any_args).and_return { |*args|
          callback = args[2]
          response = VCAP::Services::Internal::BindResponse.new
          response.success = false
          response.error = ServiceError.new(ServiceError::INTERNAL_ERROR).\
                                        to_hash
          callback.call(response.encode)
          "5"
      }
      mock_nats.should_receive(:unsubscribe).with(any_args())

      ProvisionerTests.setup_fake_instance_by_id(gateway, provisioner, "node-1")

      gateway.send_bind_request

      gateway.bind_response.should be_false
      gateway.error_msg['status'].should == 500
      gateway.error_msg['msg']['code'].should == 30500

      EM.stop
    end
  end

  it "should handle error in unbind" do
    provisioner = nil
    gateway = nil
    mock_nats = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      provisioner = ProvisionerTests.create_provisioner
      # assign mock nats to provisioner
      provisioner.nats = mock_nats
      gateway = ProvisionerTests.create_error_gateway(provisioner)
      # mock node to send unbind request
      mock_nodes = {
          "node-1" => {
              "id" => "node-1",
              "plan" => "free",
              "available_capacity" => 200,
              "capacity_unit" => 1,
              "time" => Time.now.to_i
          }
      }
      provisioner.nodes = mock_nodes

      mock_nats.should_receive(:request).with(any_args()).\
      and_return { |*args|
          callback = args[2]
          response = VCAP::Services::Internal::SimpleResponse.new
          response.success = false
          response.error = ServiceError.new(ServiceError::INTERNAL_ERROR).\
                                        to_hash
          callback.call(response.encode)
          "5"
      }
      mock_nats.should_receive(:unsubscribe).with(any_args())

      ProvisionerTests.setup_fake_instance_by_id(gateway, provisioner, "node-1")
      bind_id = "fake_bind_id"
      gateway.bind_id = bind_id
      provisioner.prov_svcs[bind_id] = {:credentials => {'node_id' => "node-1"}}

      gateway.send_unbind_request

      gateway.unbind_response.should be_false
      gateway.error_msg['status'].should == 500
      gateway.error_msg['msg']['code'].should == 30500

      EM.stop
    end
  end

  it "should support restore" do
    provisioner = nil
    gateway = nil
    mock_nats = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      provisioner = ProvisionerTests.create_provisioner
      # assign mock nats to provisioner
      provisioner.nats = mock_nats
      gateway = ProvisionerTests.create_gateway(provisioner)
      # mock node to send provision & restore request
      mock_nodes = {
          "node-1" => {
              "id" => "node-1",
              "plan" => "free",
              "available_capacity" => 200,
              "capacity_unit" => 1,
              "time" => Time.now.to_i
          }
      }
      provisioner.nodes = mock_nodes

      mock_nats.should_receive(:request).twice.with(any_args()).\
      and_return { |*args|
          provision_request = args[0]
          callback = args[2]
          if provision_request == "Test.provision.node-1"
            response = VCAP::Services::Internal::ProvisionResponse.new
            response.success = true
            response.credentials = {
                "node_id" => "node-1",
                "name" => "b66e62e8-c87a-4adf-b08b-3cd30fcdbebb"
            }
            callback.call(response.encode)
          else
            response = VCAP::Services::Internal::SimpleResponse.new
            response.success = true
            callback.call(response.encode)
          end
          "5"
      }
      mock_nats.should_receive(:unsubscribe).twice.with(any_args())

      gateway.send_provision_request
      gateway.send_restore_request

      gateway.got_restore_response.should be_true

      EM.stop
    end
  end

  it "should handle error in restore" do
    provisioner = nil
    gateway = nil
    mock_nats = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      provisioner = ProvisionerTests.create_provisioner
      # assign mock nats to provisioner
      provisioner.nats = mock_nats
      gateway = ProvisionerTests.create_error_gateway(provisioner)
      # mock node to send restore request
      mock_nodes = {
          "node-1" => {
              "id" => "node-1",
              "plan" => "free",
              "available_capacity" => 200,
              "capacity_unit" => 1,
              "time" => Time.now.to_i
          }
      }
      provisioner.nodes = mock_nodes

      mock_nats.should_receive(:request).with(any_args()).and_return { |*args|
          callback = args[2]
          response = VCAP::Services::Internal::SimpleResponse.new
          response.success = false
          response.error = ServiceError.new(ServiceError::INTERNAL_ERROR).\
                                        to_hash
          callback.call(response.encode)
          "5"
      }
      mock_nats.should_receive(:unsubscribe).with(any_args())

      ProvisionerTests.setup_fake_instance_by_id(gateway, provisioner, "node-1")

      gateway.send_restore_request

      gateway.error_msg['status'].should == 500
      gateway.error_msg['msg']['code'].should == 30500

      EM.stop
    end
  end

  it "should support recover" do
    provisioner = nil
    gateway = nil
    mock_nats = nil
    provision_request = ""
    EM.run do
      mock_nats = mock("test_mock_nats")
      provisioner = ProvisionerTests.create_provisioner
      # assign mock nats to provisioner
      provisioner.nats = mock_nats
      gateway = ProvisionerTests.create_gateway(provisioner)
      # mock node to send provision & recover request
      mock_nodes = {
          "node-1" => {
              "id" => "node-1",
              "plan" => "free",
              "available_capacity" => 200,
              "capacity_unit" => 1,
              "time" => Time.now.to_i
          }
      }
      provisioner.nodes = mock_nodes

      mock_nats.should_receive(:request).at_least(:twice).with(any_args()).\
      and_return { |*args|
          provision_request = args[0]
          callback = args[2]
          if provision_request == "Test.provision.node-1"
            response = VCAP::Services::Internal::ProvisionResponse.new
            response.success = true
            response.credentials = {
                "node_id" => "node-1",
                "name" => "b66e62e8-c87a-4adf-b08b-3cd30fcdbebb"
            }
            callback.call(response.encode)
          elsif provision_request == "Test.restore.node-1"
            response = VCAP::Services::Internal::SimpleResponse.new
            response.success = true
            callback.call(response.encode)
          elsif provision_request == "Test.bind.node-1"
            response = VCAP::Services::Internal::BindResponse.new
            response.success = true
            response.credentials = {
                "node_id" => "node-1",
                "name" => "b66e62e8-c87a-4adf-b08b-3cd30fcdbebb"
            }
            callback.call(response.encode)
          end
          "5"
      }
      mock_nats.should_receive(:unsubscribe).at_least(:twice).with(any_args())

      gateway.send_provision_request
      gateway.send_recover_request

      gateway.got_recover_response.should be_true

      EM.stop
    end
  end

  it "should support migration" do
    provisioner = nil
    gateway = nil
    mock_nats = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      provisioner = ProvisionerTests.create_provisioner
      # assign mock nats to provisioner
      provisioner.nats = mock_nats
      gateway = ProvisionerTests.create_gateway(provisioner)
      # mock node to send provision & migrate request
      mock_nodes = {
          "node-1" => {
              "id" => "node-1",
              "plan" => "free",
              "available_capacity" => 200,
              "capacity_unit" => 1,
              "time" => Time.now.to_i
          }
      }
      provisioner.nodes = mock_nodes

      mock_nats.should_receive(:request).at_least(:twice).with(any_args()).\
      and_return { |*args|
          provision_request = args[0]
          callback = args[2]
          if provision_request == "Test.provision.node-1"
            response = VCAP::Services::Internal::ProvisionResponse.new
            response.success = true
            response.credentials = {
                "node_id" => "node-1",
                "name" => "b66e62e8-c87a-4adf-b08b-3cd30fcdbebb"
            }
            callback.call(response.encode)
          elsif provision_request == "Test.disable_instance.node-1"
            response = VCAP::Services::Internal::SimpleResponse.new
            response.success = true
            callback.call(response.encode)
          end
          "5"
      }
      mock_nats.should_receive(:unsubscribe).at_least(:twice).with(any_args())

      gateway.send_provision_request
      gateway.send_migrate_request("node-1")

      gateway.got_migrate_response.should be_true

      EM.stop
    end
  end

  it "should handle error in migration" do
    provisioner = nil
    gateway = nil
    mock_nats = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      provisioner = ProvisionerTests.create_provisioner
      # assign mock nats to provisioner
      provisioner.nats = mock_nats
      gateway = ProvisionerTests.create_error_gateway(provisioner)
      # mock node to send migrate request
      mock_nodes = {
          "node-1" => {
              "id" => "node-1",
              "plan" => "free",
              "available_capacity" => 200,
              "capacity_unit" => 1,
              "time" => Time.now.to_i
          }
      }
      provisioner.nodes = mock_nodes

      mock_nats.should_receive(:request).with(any_args()).and_return { |*args|
          callback = args[2]
          response = VCAP::Services::Internal::SimpleResponse.new
          response.success = false
          response.error = ServiceError.new(ServiceError::INTERNAL_ERROR).\
                                        to_hash
          callback.call(response.encode)
          "5"
      }
      mock_nats.should_receive(:unsubscribe).with(any_args)

      ProvisionerTests.setup_fake_instance_by_id(gateway, provisioner, "node-1")

      gateway.send_migrate_request("node-1")

      gateway.error_msg['status'].should == 500
      gateway.error_msg['msg']['code'].should == 30500

      EM.stop
    end
  end

  it "should support get instance id list" do
    provisioner = nil
    gateway = nil
    mock_nats = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      provisioner = ProvisionerTests.create_provisioner
      # assign mock nats to provisioner
      provisioner.nats = mock_nats
      gateway = ProvisionerTests.create_gateway(provisioner)
      # mock node to send provision & instances request
      mock_nodes = {
          "node-1" => {
              "id" => "node-1",
              "plan" => "free",
              "available_capacity" => 200,
              "capacity_unit" => 1,
              "time" => Time.now.to_i
          }
      }
      provisioner.nodes = mock_nodes

      mock_nats.should_receive(:request).with(any_args()).and_return { |*args|
          callback = args[2]
          response = VCAP::Services::Internal::ProvisionResponse.new
          response.success = true
          response.credentials = {
              "node_id" => "node-1",
              "name" => "b66e62e8-c87a-4adf-b08b-3cd30fcdbebb"
          }
          callback.call(response.encode)
          "5"
      }
      mock_nats.should_receive(:unsubscribe).with(any_args())

      gateway.send_provision_request
      gateway.send_instances_request("node-1")

      gateway.got_instances_response.should be_true

      EM.stop
    end
  end

  it "should handle error in getting instance id list" do
    provisioner = nil
    gateway = nil
    mock_nats = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      provisioner = ProvisionerTests.create_provisioner
      # assign mock nats to provisioner
      provisioner.nats = mock_nats
      gateway = ProvisionerTests.create_error_gateway(provisioner)
      # mock node to send migrate request
      mock_nodes = {
          "node-1" => {
              "id" => "node-1",
              "plan" => "free",
              "available_capacity" => 200,
              "capacity_unit" => 1,
              "time" => Time.now.to_i
          }
      }
      provisioner.nodes = mock_nodes

      mock_nats.should_receive(:request).with(any_args()).and_return { |*args|
          callback = args[2]
          response = VCAP::Services::Internal::SimpleResponse.new
          response.success = false
          response.error = ServiceError.new(ServiceError::INTERNAL_ERROR).\
                                        to_hash
          callback.call(response.encode)
          "5"
      }
      mock_nats.should_receive(:unsubscribe).with(any_args())

      ProvisionerTests.setup_fake_instance_by_id(gateway, provisioner, "node-1")
      gateway.send_migrate_request("node-1")

      gateway.error_msg['status'].should == 500
      gateway.error_msg['msg']['code'].should == 30500

      EM.stop
    end
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
    mock_nats = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      provisioner = ProvisionerTests.create_provisioner({
        :plan_management => {
        :plans => {
        :free => {
        :allow_over_provisioning => true
      } } } })
      # assign mock nats to provisioner
      provisioner.nats = mock_nats
      gateway = ProvisionerTests.create_gateway(provisioner)
      # mock node to send provision request
      mock_nodes = {
          "node-1" => {
              "id" => "node-1",
              "plan" => "free",
              "available_capacity" => -1,
              "capacity_unit" => 1,
              "time" => Time.now.to_i
          }
      }
      provisioner.nodes = mock_nodes

      mock_nats.should_receive(:request).with(any_args()).and_return { |*args|
          callback = args[2]
          response = VCAP::Services::Internal::ProvisionResponse.new
          response.success = true
          response.credentials = {
              "node_id" => "node-1",
              "name" => "D501B915-5B50-4C3A-93B7-7E0C48B6A9FA"
          }
          callback.call(response.encode)
          "5"
      }
      mock_nats.should_receive(:unsubscribe).with(any_args())

      gateway.send_provision_request

      gateway.got_provision_response.should be_true

      EM.stop
    end
  end

  it "should not allow over provisioning when it is not configured so" do
    provisioner = nil
    gateway = nil
    mock_nats = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      provisioner = ProvisionerTests.create_provisioner({
        :plan_management => {
        :plans => {
        :free => {
        :allow_over_provisioning => false
      } } } })
      # assign mock nats to provisioner
      provisioner.nats = mock_nats
      gateway = ProvisionerTests.create_gateway(provisioner)
      # mock node to send provision request
      mock_nodes = {
          "node-1" => {
              "id" => "node-1",
              "plan" => "free",
              "available_capacity" => -1,
              "capacity_unit" => 1,
              "time" => Time.now.to_i
          }
      }
      provisioner.nodes = mock_nodes

      gateway.send_provision_request

      gateway.got_provision_response.should be_false

      EM.stop
    end
  end

  it "should support check orphan" do
    provisioner = nil
    gateway = nil
    mock_nats = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      provisioner = ProvisionerTests.create_provisioner
      # assign mock nats to provisioner
      provisioner.nats = mock_nats
      gateway = ProvisionerTests.create_gateway(provisioner)

      mock_nats.should_receive(:publish).with("Test.check_orphan", \
                                              "Send Me Handles")
      gateway.send_check_orphan_request

      msg1 = VCAP::Services::Internal::NodeHandlesReport.new
      msg1.node_id = "node-2"
      ins_list = Array.new(2) { |i| (2 * 10 + i).to_s.ljust(36, "I") }
      bind_list = Array.new(2) do |i|
        {
          "name" => (2 * 10 + i).to_s.ljust(36, "I"),
          "username" => (2 * 10 + i).to_s.ljust(18, "U"),
          "port" => i * 1000 + 1,
          "db" => "db2"
        }
      end
      msg1.instances_list = ins_list
      msg1.bindings_list = bind_list
      msg2 = VCAP::Services::Internal::NodeHandlesReport.new
      msg2.node_id = "node-3"
      ins_list = Array.new(3) { |i| (3 * 10 + i).to_s.ljust(36, "I") }
      bind_list = Array.new(3) do |i|
        {
          "name" => (3 * 10 + i).to_s.ljust(36, "I"),
          "username" => (3 * 10 + i).to_s.ljust(18, "U"),
          "port" => i * 1000 + 1,
          "db" => "db3"
        }
      end
      msg2.instances_list = ins_list
      msg2.bindings_list = bind_list
      # mock nats subscribe callback function only can be invoked manually
      provisioner.on_node_handles(msg1.encode, nil)
      provisioner.on_node_handles(msg2.encode, nil)

      gateway.send_double_check_orphan_request

      provisioner.staging_orphan_instances["node-2"].count.should == 2
      provisioner.staging_orphan_instances["node-3"].count.should == 2
      provisioner.final_orphan_instances["node-2"].count.should == 1
      provisioner.final_orphan_instances["node-3"].count.should == 2
      provisioner.staging_orphan_bindings["node-2"].count.should == 1
      provisioner.staging_orphan_bindings["node-3"].count.should == 2
      provisioner.final_orphan_bindings["node-2"].count.should == 1
      provisioner.final_orphan_bindings["node-3"].count.should == 2

      EM.stop
    end
  end

  it "should handle error in check orphan" do
    provisioner = nil
    gateway = nil
    mock_nats = nil
    EM.run do
      mock_nats = mock("test_mock_nats")
      provisioner = ProvisionerTests.create_provisioner
      # assign mock nats to provisioner
      provisioner.nats = mock_nats
      gateway = ProvisionerTests.create_error_gateway(provisioner)

      mock_nats.should_receive(:publish).with("Test.check_orphan", \
                                              "Send Me Handles")
      gateway.send_check_orphan_request

      node = VCAP::Services::Internal::NodeHandlesReport.new
      node.node_id = "mailformed_node"
      node.instances_list = ["malformed-due-to-no-bindings-list"]
      node.bindings_list = nil
      # mock nats subscribe callback function only can be invoked manually
      provisioner.on_node_handles(node.encode, nil)

      provisioner.staging_orphan_instances["node-1"].should be_nil
      provisioner.final_orphan_instances["node-1"].should be_nil

      EM.stop
    end
  end

  it "should support purging massive orphans" do
    provisioner = nil
    gateway = nil
    mock_nats = nil
    purge_ins_list = []
    purge_bind_list = []
    EM.run do
      mock_nats = mock("test_mock_nats")
      provisioner = ProvisionerTests.create_provisioner
      # assign mock nats to provisioner
      provisioner.nats = mock_nats
      gateway = ProvisionerTests.create_gateway(provisioner, \
                                                1024 * 128, \
                                                1024 * 16)

      mock_nats.should_receive(:publish).at_least(:once).with(any_args).\
      and_return { |*args|
          req = VCAP::Services::Internal::PurgeOrphanRequest.decode(args[1])
          purge_ins_list.concat(req.orphan_ins_list)
          purge_bind_list.concat(req.orphan_binding_list)
      }
      gateway.send_purge_orphan_request

      gateway.got_purge_orphan_response.should be_true
      purge_ins_list.count.should == 1024 * 128
      purge_bind_list.count.should == 1024 * 16

      EM.stop
    end
  end

end
