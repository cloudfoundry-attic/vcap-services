require 'base/node'

class NodeTests

  def self.create_node(options={})
    NodeTester.new(BaseTests::Options.default({:node_id => NodeTester::ID}).merge(options))
  end

  def self.create_error_node
    NodeErrorTester.new(BaseTests::Options.default({:node_id => NodeTester::ID}))
  end

  def self.create_error_provisioner
    MockErrorProvisioner.new
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
    attr_reader   :unprovision_count
    attr_reader   :unbind_count
    attr_reader   :capacity
    attr_accessor :varz_invoked
    attr_accessor :healthz_invoked
    SERVICE_NAME = "Test"
    ID = "node-1"
    def initialize(options)
      super(options)
      @ready = true
      @announcement_invoked = false
      @provision_invoked = false
      @unprovision_invoked = false
      @bind_invoked = false
      @unbind_invoked = false
      @restore_invoked = false
      @provision_times = 0
      @mutex = Mutex.new
      @unprovision_count = 0
      @unbind_count = 0
      @varz_invoked = false
      @healthz_invoked = false
      @ins_count = options[:ins_count] || 0
      @bind_count = options[:bind_count] || 0
      @plan = options[:plan] || "free"
    end
    def service_name
      SERVICE_NAME
    end
    def set_ready(r)
      @ready = r
    end
    def node_ready?
      @ready
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
      @mutex.synchronize{ @unprovision_count += 1 }
    end
    def bind(name, bind_opts, credential)
      @bind_invoked = true
    end
    def unbind(credentials)
      @unbind_invoked = true
      @mutex.synchronize{ @unbind_count += 1 }
    end
    def restore(isntance_id, backup_path)
      @restore_invoked = true
    end
    def varz_details
      @varz_invoked = true
      {}
    end
    def healthz_details
      @healthz_invoked = true
      {}
    end

    def all_instances_list
      generate_ins_list(@ins_count)
    end

    def all_bindings_list
      generate_bind_list(@bind_count)
    end

  end

  class MockProvisioner
    include VCAP::Services::Internal
    attr_accessor :got_announcement
    attr_accessor :got_announcement_by_plan
    attr_accessor :got_provision_response
    attr_accessor :ins_hash
    attr_accessor :bind_hash
    def initialize
      @got_announcement = false
      @got_announcement_by_plan = false
      @got_provision_response = false
      @got_unprovision_response = false
      @got_bind_response = false
      @got_unbind_response = false
      @got_restore_response = false
      @ins_hash = Hash.new { |h,k| h[k] = [] }
      @bind_hash = Hash.new { |h,k| h[k] = [] }
      @nats = NATS.connect(:uri => BaseTests::Options::NATS_URI) {
        @nats.subscribe("#{NodeTester::SERVICE_NAME}.announce") {
          @got_announcement = true
        }
        @nats.subscribe("#{NodeTester::SERVICE_NAME}.node_handles") do |msg|
          response = NodeHandlesReport.decode(msg)
          @ins_hash[response.node_id].concat(response.instances_list)
          @bind_hash[response.node_id].concat(response.bindings_list)
        end
        @nats.publish("#{NodeTester::SERVICE_NAME}.discover")
      }
    end
    def send_discover_by_plan(plan)
      req = Yajl::Encoder.encode({"plan" => plan})
      @nats.request("#{NodeTester::SERVICE_NAME}.discover", req) {|_| @got_announcement_by_plan = true }
    end
    def send_provision_request
      req = ProvisionRequest.new
      req.plan = "free"
      @nats.request("#{NodeTester::SERVICE_NAME}.provision.#{NodeTester::ID}", req.encode) {
        @got_provision_response = true
      }
    end
    def send_unprovision_request
      req = UnprovisionRequest.new
      req.name = "fake"
      req.bindings = []
      @nats.request("#{NodeTester::SERVICE_NAME}.unprovision.#{NodeTester::ID}", req.encode ) {
        @got_unprovision_response = true
      }
    end
    def send_bind_request
      req = BindRequest.new
      req.name = "fake"
      req.bind_opts = {}
      @nats.request("#{NodeTester::SERVICE_NAME}.bind.#{NodeTester::ID}", req.encode) {
        @got_bind_response = true
      }
    end
    def send_unbind_request
      req = UnbindRequest.new
      req.credentials = {}
      @nats.request("#{NodeTester::SERVICE_NAME}.unbind.#{NodeTester::ID}", req.encode) {
        @got_unbind_response = true
      }
    end
    def send_restore_request
      req = RestoreRequest.new
      req.instance_id = "fake1"
      req.backup_path = "/tmp"
      @nats.request("#{NodeTester::SERVICE_NAME}.restore.#{NodeTester::ID}", req.encode) {
        @got_restore_response = true
      }
    end
    def send_check_orphan_request
      @nats.publish("#{NodeTester::SERVICE_NAME}.check_orphan", "send me handles")
    end
    def send_purge_orphan_request
      req = PurgeOrphanRequest.new
      req.orphan_ins_list = TEST_PURGE_INS_HASH[TEST_NODE_ID]
      req.orphan_binding_list = TEST_PURGE_BIND_HASH[TEST_NODE_ID]
      @nats.publish("#{NodeTester::SERVICE_NAME}.purge_orphan.#{NodeTester::ID}", req.encode)
    end
  end

  # Test Node which raise error
  class NodeErrorTester < VCAP::Services::Base::Node
    include VCAP::Services::Base::Error
    attr_accessor :announcement_invoked
    attr_accessor :provision_invoked
    attr_accessor :unprovision_invoked
    attr_accessor :bind_invoked
    attr_accessor :unbind_invoked
    attr_accessor :restore_invoked
    attr_accessor :check_orphan_invoked
    attr_accessor :provision_times
    attr_reader   :capacity
    SERVICE_NAME = "Test"
    ID = "node-error"
    def initialize(options)
      super(options)
      @announcement_invoked = false
      @provision_invoked = false
      @unprovision_invoked = false
      @bind_invoked = false
      @unbind_invoked = false
      @restore_invoked = false
      @check_orphan_invoked = false
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
      @provision_invoked = true
      raise ServiceError.new(ServiceError::SERVICE_UNAVAILABLE)
    end
    def unprovision(name, bindings)
      @unprovision_invoked = true
      raise ServiceError.new(ServiceError::SERVICE_UNAVAILABLE)
    end
    def bind(name, bind_opts, credential)
      @bind_invoked = true
      raise ServiceError.new(ServiceError::SERVICE_UNAVAILABLE)
    end
    def unbind(credentials)
      @unbind_invoked = true
      raise ServiceError.new(ServiceError::SERVICE_UNAVAILABLE)
    end
    def restore(isntance_id, backup_path)
      @restore_invoked = true
      raise ServiceError.new(ServiceError::SERVICE_UNAVAILABLE)
    end
    def check_orphan(handles)
      @check_orphan_invoked = true
      raise ServiceError.new(ServiceError::SERVICE_UNAVAILABLE)
    end
  end

  # Provisioner that catch error from node
  class MockErrorProvisioner < MockProvisioner
    include VCAP::Services::Internal
    attr_accessor :got_announcement
    attr_accessor :got_provision_response
    attr_accessor :got_unprovision_response
    attr_accessor :got_bind_response
    attr_accessor :got_unbind_response
    attr_accessor :got_restore_response
    attr_accessor :response
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
      @response = nil
    end
    def send_provision_request
      req = ProvisionRequest.new
      req.plan = "free"
      @nats.request("#{NodeTester::SERVICE_NAME}.provision.#{NodeTester::ID}", req.encode) do |msg|
        @got_provision_response = true
        @response = msg
      end
    end
    def send_unprovision_request
      req = UnprovisionRequest.new
      req.name = "fake"
      req.bindings = []
      @nats.request("#{NodeTester::SERVICE_NAME}.unprovision.#{NodeTester::ID}", req.encode ) do |msg|
        @got_unprovision_response = true
        @response = msg
      end
    end
    def send_bind_request
      req = BindRequest.new
      req.name = "fake"
      req.bind_opts = {}
      @nats.request("#{NodeTester::SERVICE_NAME}.bind.#{NodeTester::ID}", req.encode) do |msg|
        @got_bind_response = true
        @response = msg
      end
    end
    def send_unbind_request
      req = UnbindRequest.new
      req.credentials = {}
      @nats.request("#{NodeTester::SERVICE_NAME}.unbind.#{NodeTester::ID}", req.encode) do |msg|
        @got_unbind_response = true
        @response = msg
      end
    end
    def send_restore_request
      req = RestoreRequest.new
      req.instance_id = "fake1"
      req.backup_path = "/tmp"
      @nats.request("#{NodeTester::SERVICE_NAME}.restore.#{NodeTester::ID}", req.encode) do |msg|
        @got_restore_response = true
        @response = msg
      end
    end
  end
end
