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
    attr_accessor :varz_invoked
    def initialize(options)
      @node_mbus_connected = false
      @varz_invoked = false
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
    def varz_details
      @varz_invoked = true
      {}
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
    attr_accessor :bind_invoked
    attr_accessor :unbind_invoked
    attr_accessor :restore_invoked
    attr_accessor :provision_times
    SERVICE_NAME = "Test"
    ID = "node-1"
    def initialize(options)
      super(options)
      @announcement_invoked = false
      @provision_invoked = false
      @unprovision_invoked = false
      @bind_invoked = false
      @unbind_invoked = false
      @restore_invoked = false
      @provision_times = 0
      @mutex = Mutex.new
    end
    def service_name
      SERVICE_NAME
    end
    def announcement
      @announcement_invoked = true
      Hash.new
    end
    def provision(plan, credential)
      sleep 5 # Provision takes 5 seconds to finish
      @mutex.synchronize { @provision_times += 1 }
      @provision_invoked = true
      Hash.new
    end
    def unprovision(name, bindings)
      @unprovision_invoked = true
    end
    def bind(name, bind_opts, credential)
      @bind_invoked = true
    end
    def unbind(credentials)
      @unbind_invoked = true
    end
    def restore(isntance_id, backup_path)
      @restore_invoked = true
    end
  end

  class MockProvisioner
    attr_accessor :got_announcement
    attr_accessor :got_provision_response
    def initialize
      @got_announcement = false
      @got_provision_response = false
      @got_unprovision_response = false
      @got_bind_response = false
      @got_unbind_response = false
      @got_restore_response = false
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
      @nats.request("#{NodeTester::SERVICE_NAME}.unprovision.#{NodeTester::ID}", "{}") {
        @got_unprovision_response = true
      }
    end
    def send_bind_request
      @nats.request("#{NodeTester::SERVICE_NAME}.bind.#{NodeTester::ID}", "{}") {
        @got_bind_response = true
      }
    end
    def send_unbind_request
      @nats.request("#{NodeTester::SERVICE_NAME}.unbind.#{NodeTester::ID}", "{}") {
        @got_unbind_response = true
      }
    end
    def send_restore_request
      @nats.request("#{NodeTester::SERVICE_NAME}.restore.#{NodeTester::ID}", "{}") {
        @got_restore_response = true
      }
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
    attr_accessor :prov_svcs
    attr_accessor :varz_invoked
    attr_accessor :prov_svcs
    def initialize(options)
      super(options)
      @varz_invoked = false
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
    def varz_details
      @varz_invoked = true
      super
    end
  end

  class MockGateway
    attr_accessor :got_announcement
    attr_accessor :got_provision_response
    attr_accessor :got_unprovision_response
    attr_accessor :got_bind_response
    attr_accessor :got_unbind_response
    attr_accessor :got_restore_response
    attr_accessor :got_recover_response
    def initialize(provisioner)
      @provisioner = provisioner
      @got_announcement = false
      @got_provision_response = false
      @got_unprovision_response = false
      @got_bind_response = false
      @got_unbind_response = false
      @got_restore_response = false
      @got_recover_response = false
      @instance_id = nil
      @bind_id = nil
    end
    def send_provision_request
      @provisioner.provision_service({}, nil) do |res|
        @instance_id = res['response'][:service_id]
        @got_provision_response = res['success']
      end
    end
    def send_unprovision_request
      @provisioner.unprovision_service(@instance_id) do |res|
        @got_unprovision_response = res['success']
      end
    end
    def send_bind_request
      @provisioner.bind_instance(@instance_id, nil, nil) do |res|
        @bind_id = res['response'][:service_id]
        @got_bind_response = res['success']
      end
    end
    def send_unbind_request
      @provisioner.unbind_instance(@instance_id, @bind_id, nil) do |res|
        @got_unbind_response = res['success']
      end
    end
    def send_restore_request
      @provisioner.restore_instance(@instance_id, nil) do |res|
        @got_restore_response = res['success']
      end
    end
    def send_recover_request
      @provisioner.recover(@instance_id, nil, [{'service_id' => @instance_id, 'configuration' => {}}]) do |res|
        @got_recover_response = res['success']
      end
    end
  end

  class MockNode
    attr_accessor :got_unprovision_request
    attr_accessor :got_provision_request
    attr_accessor :got_unbind_request
    attr_accessor :got_bind_request
    attr_accessor :got_restore_request
    def initialize(id)
      @id = id
      @got_provision_request = false
      @got_unprovision_request = false
      @got_bind_request = false
      @got_unbind_request = false
      @got_restore_request = false
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
              'node_id' => node_id,
              'username' => UUIDTools::UUID.random_create.to_s,
              'password' => UUIDTools::UUID.random_create.to_s,
            }
          }
          @nats.publish(reply, response.to_json)
        }
        @nats.subscribe("#{service_name}.unprovision.#{node_id}") { |msg, reply|
          @got_unprovision_request = true
          response = {
            'success' => true,
            'response' => {}
          }
          @nats.publish(reply, response.to_json)
        }
        @nats.subscribe("#{service_name}.bind.#{node_id}") { |msg, reply|
          @got_bind_request = true
          response = {
            'success' => true,
            'response' => {
              'name' => UUIDTools::UUID.random_create.to_s,
              'node_id' => node_id,
              'username' => UUIDTools::UUID.random_create.to_s,
              'password' => UUIDTools::UUID.random_create.to_s,
            }
          }
          @nats.publish(reply, response.to_json)
        }
        @nats.subscribe("#{service_name}.unbind.#{node_id}") { |msg, reply|
          @got_unbind_request = true
          response = {
            'success' => true,
            'response' => {}
          }
          @nats.publish(reply, response.to_json)
        }
        @nats.subscribe("#{service_name}.restore.#{node_id}") { |msg, reply|
          @got_restore_request = true
          response = {
            'success' => true,
            'response' => {}
          }
          @nats.publish(reply, response.to_json)
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

require 'base/asynchronous_service_gateway'

class AsyncGatewayTests
  CC_PORT = 34512
  GW_PORT = 34513

  def self.create_nice_gateway
    MockGateway.new(true)
  end

  def self.create_nasty_gateway
    MockGateway.new(false)
  end

  def self.create_cloudcontroller
    MockCloudController.new
  end

  class MockGateway
    attr_accessor :provision_http_code
    attr_accessor :unprovision_http_code
    attr_accessor :bind_http_code
    attr_accessor :unbind_http_code
    attr_accessor :restore_http_code
    attr_accessor :recover_http_code

    def initialize(nice)
      @token = '0xdeadbeef'
      @cc_head = {
        'Content-Type'         => 'application/json',
        'X-VCAP-Service-Token' => @token,
      }
      @label = "service-1.0"
      sp = nice ? NiceProvisioner.new : NastyProvisioner.new
      sg = VCAP::Services::AsynchronousServiceGateway.new(
        :service => {
                      :label => @label,
                      :name => 'service',
                      :version => '1.0',
                      :description => 'sample desc',
                      :plans => ['free'],
                      :tags => ['nosql']
                    },
        :token   => @token,
        :provisioner => sp,
        :cloud_controller_uri => "http://localhost:#{CC_PORT}"
      )
      @server = Thin::Server.new('localhost', GW_PORT, sg)
      @provision_http_code = 0
      @unprovision_http_code = 0
      @bind_http_code = 0
      @unbind_http_code = 0
      @restore_http_code = 0
      @recover_http_code = 0
      @last_service_id = nil
      @last_bind_id = nil
    end

    def start
      Thread.new { @server.start }
    end

    def stop
      @server.stop
    end

    def gen_req(body = nil)
      req = { :head => @cc_head }
      req[:body] = body if body
      req
    end

    def send_provision_request
      msg = Yajl::Encoder.encode({
        :label => @label,
        :plan  => "free"
      })
      http = EM::HttpRequest.new("http://localhost:#{GW_PORT}/gateway/v1/configurations").post(gen_req(msg))
      http.callback {
        @provision_http_code = http.response_header.status
        res = Yajl::Parser.parse(http.response)
        @last_service_id = res['service_id']
      }
      http.errback {
        @provision_http_code = -1
      }
    end

    def send_unprovision_request(service_id = nil)
      service_id ||= @last_service_id
      http = EM::HttpRequest.new("http://localhost:#{GW_PORT}/gateway/v1/configurations/#{service_id}").delete(gen_req)
      http.callback {
        @unprovision_http_code = http.response_header.status
      }
      http.errback {
        @unprovision_http_code = -1
      }
    end

    def send_bind_request(service_id = nil)
      service_id ||= @last_service_id
      msg = Yajl::Encoder.encode({
        :service_id => service_id,
        :label => @label,
        :binding_options => {}
      })
      http = EM::HttpRequest.new("http://localhost:#{GW_PORT}/gateway/v1/configurations/#{service_id}/handles").post(gen_req(msg))
      http.callback {
        @bind_http_code = http.response_header.status
        res = Yajl::Parser.parse(http.response)
        @last_bind_id = res['service_id']
      }
      http.errback {
        @bind_http_code = -1
      }
    end

    def send_unbind_request(service_id = nil, bind_id = nil)
      service_id ||= @last_service_id
      bind_id ||= @last_bind_id
      msg = Yajl::Encoder.encode({
        :service_id => service_id,
        :handle_id => bind_id,
        :binding_options => {}
      })
      http = EM::HttpRequest.new("http://localhost:#{GW_PORT}/gateway/v1/configurations/#{service_id}/handles/#{bind_id}").delete(gen_req(msg))
      http.callback {
        @unbind_http_code = http.response_header.status
      }
      http.errback {
        @unbind_http_code = -1
      }
    end

    def send_restore_request(service_id = nil)
      service_id ||= @last_service_id
      msg = Yajl::Encoder.encode({
        :instance_id => service_id,
        :backup_path => '/'
      })
      http = EM::HttpRequest.new("http://localhost:#{GW_PORT}/service/internal/v1/restore").post(gen_req(msg))
      http.callback {
        @restore_http_code = http.response_header.status
      }
      http.errback {
        @restore_http_code = -1
      }
    end

    def send_recover_request(service_id = nil)
      service_id ||= @last_service_id
      msg = Yajl::Encoder.encode({
        :instance_id => service_id,
        :backup_path => '/'
      })
      http = EM::HttpRequest.new("http://localhost:#{GW_PORT}/service/internal/v1/recover").post(gen_req(msg))
      http.callback {
        @recover_http_code = http.response_header.status
      }
      http.errback {
        @recover_http_code = -1
      }
    end
  end

  class MockCloudController
    def initialize
      @server = Thin::Server.new('localhost', CC_PORT, Handler.new)
    end

    def start
      Thread.new { @server.start }
    end

    def stop
      @server.stop if @server
    end

    class Handler < Sinatra::Base
      post "/services/v1/offerings" do
        "{}"
      end

      get "/services/v1/offerings/:label/handles" do
        Yajl::Encoder.encode({
          :handles => [{
            'service_id' => MockProvisioner::SERV_ID,
            'configuration' => {},
            'credentials' => {}
          }]
        })
      end

      get "/services/v1/offerings/:label/handles/:id" do
        "{}"
      end
    end
  end

  class MockProvisioner
    SERV_ID = "service_id"
    BIND_ID = "bind_id"

    include VCAP::Services::Base::Error

    attr_accessor :got_provision_request
    attr_accessor :got_unprovision_request
    attr_accessor :got_bind_request
    attr_accessor :got_unbind_request
    attr_accessor :got_restore_request
    attr_accessor :got_recover_request

    def initialize
      @got_provision_request = false
      @got_unprovision_request = false
      @got_bind_request = false
      @got_unbind_request = false
      @got_restore_request = false
      @got_recover_request = false
    end

    def register_update_handle_callback
      # Do nothing
    end

    def update_handles(handles)
      # Do nothing
    end
  end

  class NiceProvisioner < MockProvisioner
    def provision_service(request, prov_handle=nil, &blk)
      @got_provision_request = true
      blk.call(success({:data => {}, :service_id => SERV_ID, :credentials => {}}))
    end

    def unprovision_service(instance_id, &blk)
      @got_unprovision_request = true
      blk.call(success(true))
    end

    def bind_instance(instance_id, binding_options, bind_handle=nil, &blk)
      @got_bind_request = true
      blk.call(success({:configuration => {}, :service_id => BIND_ID, :credentials => {}}))
    end

    def unbind_instance(instance_id, handle_id, binding_options, &blk)
      @got_unbind_request = true
      blk.call(success(true))
    end

    def restore_instance(instance_id, backup_path, &blk)
      @got_restore_request = true
      blk.call(success(true))
    end

    def recover(instance_id, backup_path, handles, &blk)
      @got_recover_reqeust = true
      blk.call(success(true))
    end
  end

  class NastyProvisioner < MockProvisioner
    def provision_service(request, prov_handle=nil, &blk)
      @got_provision_request = true
      blk.call(internal_fail)
    end

    def unprovision_service(instance_id, &blk)
      @got_unprovision_request = true
      blk.call(internal_fail)
    end

    def bind_instance(instance_id, binding_options, bind_handle=nil, &blk)
      @got_bind_request = true
      blk.call(internal_fail)
    end

    def unbind_instance(instance_id, handle_id, binding_options, &blk)
      @got_unbind_request = true
      blk.call(internal_fail)
    end

    def restore_instance(instance_id, backup_path, &blk)
      @got_restore_request = true
      blk.call(internal_fail)
    end

    def recover(instance_id, backup_path, handles, &blk)
      @got_recover_reqeust = true
      blk.call(internal_fail)
    end
  end
end
