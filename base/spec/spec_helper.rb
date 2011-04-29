# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), '..')
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'rubygems'
require 'rspec'
require 'logger'

require 'base/base'

class BaseTests

  module Options

    LOGGER = Logger.new(STDOUT)

    # tests use this message bus to avoid pollution/contamination of a
    # "real" message bus that might be in use
    NATS_URI = "nats://localhost:9222"

    IP_ROUTE = "127.0.0.1"

    def self.default(more=nil)
      options = {
        :logger => LOGGER,
        :ip_route => IP_ROUTE,
        :mbus => NATS_URI,
      }
      more.each { |k,v| options[k] = v } if more
      options
    end

  end

  def self.create_base
    BaseTester.new(Options.default)
  end

  class BaseTester < VCAP::Services::Base::Base
    attr_accessor :node_mbus_connected
    def initialize(options)
      @node_mbus_connected = false
      super(options)
    end
    def flavor
      "flavor"
    end
    def service_name
      "service_name"
    end
    def on_connect_node
      @node_mbus_connected = true
    end
  end

end

require 'base/node'

class NodeTests

  def self.create_node
    NodeTester.new(BaseTests::Options.default({:node_id => NodeTester::ID}))
  end

  def self.create_provisioner
    MockProvisioner.new
  end

  class NodeTester < VCAP::Services::Base::Node
    attr_accessor :announcement_invoked
    attr_accessor :provision_invoked
    attr_accessor :unprovision_invoked
    SERVICE_NAME = "Test"
    ID = "node-1"
    def initialize(options)
      super(options)
      @announcement_invoked = false
      @provision_invoked = false
      @unprovision_invoked = false
    end
    def service_name
      SERVICE_NAME
    end
    def announcement
      @announcement_invoked = true
      Hash.new
    end
    def provision(plan)
      @provision_invoked = true
      Hash.new
    end
    def unprovision(name, bindings)
      @unprovision_invoked = true
    end
  end

  class MockProvisioner
    attr_accessor :got_announcement
    attr_accessor :got_provision_response
    def initialize
      @got_announcement = false
      @got_provision_response = false
      @nats = NATS.connect(:uri => BaseTests::Options::NATS_URI) {
        @nats.subscribe("#{NodeTester::SERVICE_NAME}.announce") {
          @got_announcement = true
        }
        @nats.publish("#{NodeTester::SERVICE_NAME}.discover")
      }
    end
    def send_provision_request
      @nats.request("#{NodeTester::SERVICE_NAME}.provision.#{NodeTester::ID}", "{}") {
        @got_provision_response = true
      }
    end
    def send_unprovision_request
      @nats.publish("#{NodeTester::SERVICE_NAME}.unprovision.#{NodeTester::ID}", "{}")
    end
  end

end

require 'base/provisioner'

class ProvisionerTests

  def self.create_provisioner
    ProvisionerTester.new(BaseTests::Options.default)
  end

  def self.create_gateway(provisioner)
    MockGateway.new(provisioner)
  end

  def self.create_node(id)
    MockNode.new(id)
  end

  class ProvisionerTester < VCAP::Services::Base::Provisioner
    def initialize(options)
      super(options)
    end
    SERVICE_NAME = "Test"
    def service_name
      SERVICE_NAME
    end
    def node_score(node)
      node["score"]
    end
    def node_count
      return @nodes.length
    end
    def first_instance_id
      @prov_svcs.keys[0]
    end
  end

  class MockGateway
    attr_accessor :got_announcement
    attr_accessor :got_provision_response
    def initialize(provisioner)
      @provisioner = provisioner
      @got_announcement = false
      @got_provision_response = false
    end
    def send_provision_request
      @provisioner.provision_service(nil, nil) { @got_provision_response = true }
    end
    def send_unprovision_request
      @provisioner.unprovision_service(@provisioner.first_instance_id) { }
    end
  end

  class MockNode
    attr_accessor :got_unprovision_request
    attr_accessor :got_provision_request
    def initialize(id)
      @id = id
      @got_provision_request = false
      @got_unprovision_request = false
      @nats = NATS.connect(:uri => BaseTests::Options::NATS_URI) {
        @nats.subscribe("#{service_name}.discover") { |_, reply|
          announce(reply)
        }
        @nats.subscribe("#{service_name}.provision.#{node_id}") { |_, reply|
          @got_provision_request = true
          response = {
            'success' => true,
            'response' => {
              'name' => UUIDTools::UUID.random_create.to_s,
              'node_id' => node_id
            }
          }
          @nats.publish(reply, response.to_json)
        }
        @nats.subscribe("#{service_name}.unprovision.#{node_id}") { |msg, reply|
          @got_unprovision_request = true
        }
        announce
      }
    end
    def service_name
      ProvisionerTester::SERVICE_NAME
    end
    def node_id
      "node-#{@id}"
    end
    def announce(reply=nil)
      a = { :id => node_id, :score => @id }
      @nats.publish(reply||"#{service_name}.announce", a.to_json)
    end
  end

end
