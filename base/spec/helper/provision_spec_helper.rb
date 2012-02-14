require 'base/provisioner'

class ProvisionerTests

  def self.create_provisioner(options = {})
    ProvisionerTester.new(BaseTests::Options.default(options))
  end

  def self.create_gateway(provisioner, ins_count=1, bind_count=1)
    MockGateway.new(provisioner, ins_count, bind_count)
  end

  def self.create_error_gateway(provisioner, ins_count=1, bind_count=1)
    MockErrorGateway.new(provisioner, ins_count, bind_count)
  end

  def self.create_node(id, score = 1)
    MockNode.new(id, score)
  end

  def self.create_error_node(id, score = 1)
    MockErrorNode.new(id, score)
  end

  def self.setup_fake_instance(gateway, provisioner, node)
    instance_id = "fake_instance"
    gateway.instance_id = instance_id
    provisioner.prov_svcs[instance_id] = {:credentials => {'node_id' =>node.node_id }}
  end

  class ProvisionerTester < VCAP::Services::Base::Provisioner
    attr_accessor :prov_svcs
    attr_accessor :varz_invoked
    attr_accessor :healthz_invoked
    attr_accessor :prov_svcs
    attr_reader   :staging_orphan_instances
    attr_reader   :staging_orphan_bindings
    attr_reader   :final_orphan_instances
    attr_reader   :final_orphan_bindings
    def initialize(options)
      super(options)
      @varz_invoked = false
      @healthz_invoked = false
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
    def healthz_details
      @healthz_invoked = true
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
    attr_reader :got_purge_orphan_response
    attr_reader :got_check_orphan_response
    def initialize(provisioner, ins_count, bind_count)
      @provisioner = provisioner
      @got_announcement = false
      @got_provision_response = false
      @got_unprovision_response = false
      @got_bind_response = false
      @got_unbind_response = false
      @got_restore_response = false
      @got_recover_response = false
      @got_purge_orphan_response = false
      @got_check_orphan_response = false
      @instance_id = nil
      @bind_id = nil
      @ins_count = ins_count
      @bind_count = bind_count
    end
    def send_provision_request
      req = VCAP::Services::Api::GatewayProvisionRequest.new
      req.plan = "free"
      @provisioner.provision_service(req, nil) do |res|
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
      @provisioner.bind_instance(@instance_id, {}, nil) do |res|
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
      # register a fake callback to provisioner which always return true
      @provisioner.register_update_handle_callback{|handle, &blk| blk.call(true)}
      @provisioner.recover(@instance_id, "/tmp", [{'service_id' => @instance_id, 'configuration' => {'plan' => 'free'}},{'service_id' => 'fake_uuid', 'configuration' => {}, 'credentials' => {'name' => @instance_id}}]) do |res|
        @got_recover_response = res['success']
      end
    end
    def send_check_orphan_request
      @provisioner.check_orphan(TEST_CHECK_HANDLES.drop(1)) do |res|
        @got_check_orphan_response = res["success"]
      end
    end
    def send_double_check_orphan_request
      @provisioner.double_check_orphan(TEST_CHECK_HANDLES)
    end
    def send_purge_orphan_request
      @provisioner.purge_orphan(
        {TEST_NODE_ID => generate_ins_list(@ins_count)},
        {TEST_NODE_ID => generate_bind_list(@bind_count)}) do |res|
        @got_purge_orphan_response = res['success']
      end
    end
  end

  # Gateway that catch error from node
  class MockErrorGateway < MockGateway
    attr_accessor :got_announcement
    attr_accessor :provision_response
    attr_accessor :unprovision_response
    attr_accessor :bind_response
    attr_accessor :unbind_response
    attr_accessor :restore_response
    attr_accessor :recover_response
    attr_accessor :error_msg
    attr_accessor :instance_id
    attr_accessor :bind_id
    def initialize(provisioner, ins_count, bind_count)
      @provisioner = provisioner
      @got_announcement = false
      @provision_response = true
      @unprovision_response = true
      @bind_response = true
      @unbind_response = true
      @restore_response = true
      @recover_response = true
      @error_msg = nil
      @instance_id = nil
      @bind_id = nil
      @ins_count = ins_count
      @bind_count = bind_count
    end
    def send_provision_request(plan="free")
      req = VCAP::Services::Api::GatewayProvisionRequest.new
      req.plan = plan
      @provisioner.provision_service(req, nil) do |res|
        @provision_response = res['success']
        @error_msg = res['response']
      end
    end
    def send_unprovision_request
      @provisioner.unprovision_service(@instance_id) do |res|
        @unprovision_response = res['success']
        @error_msg = res['response']
      end
    end
    def send_bind_request
      @provisioner.bind_instance(@instance_id, {}, nil) do |res|
        @bind_response = res['success']
        @bind_id = res['response'][:service_id]
        @bind_response = res['success']
        @error_msg = res['response']
      end
    end
    def send_unbind_request
      @provisioner.unbind_instance(@instance_id, @bind_id, nil) do |res|
        @unbind_response = res['success']
        @error_msg = res['response']
      end
    end
    def send_restore_request
      @provisioner.restore_instance(@instance_id, nil) do |res|
        @restore_response = res['success']
        @error_msg = res['response']
      end
    end
    def send_recover_request
      # register a fake callback to provisioner which always return true
      @provisioner.register_update_handle_callback{|handle, &blk| blk.call(true)}
      @provisioner.recover(@instance_id, "/tmp", [{'service_id' => @instance_id, 'configuration' => {'plan' => 'free'}},{'service_id' => 'fake_uuid', 'configuration' => {}, 'credentials' => {'name' => @instance_id}}]) do |res|
        @recover_response = res['success']
        @error_msg = res['response']
      end
    end
  end

  class MockNode
    include VCAP::Services::Internal
    attr_accessor :got_unprovision_request
    attr_accessor :got_provision_request
    attr_accessor :got_unbind_request
    attr_accessor :got_bind_request
    attr_accessor :got_restore_request
    attr_reader :got_check_orphan_request
    attr_reader :got_purge_orphan_request
    attr_reader :purge_ins_list
    attr_reader :purge_bind_list
    def initialize(id, score)
      @id = id
      @plan = "free"
      @score = score
      @got_provision_request = false
      @got_unprovision_request = false
      @got_bind_request = false
      @got_unbind_request = false
      @got_restore_request = false
      @got_check_orphan_request = false
      @got_purge_orphan_request = false
      @purge_ins_list = []
      @purge_bind_list = []
      @nats = NATS.connect(:uri => BaseTests::Options::NATS_URI) {
        @nats.subscribe("#{service_name}.discover") { |_, reply|
          announce(reply)
        }
        @nats.subscribe("#{service_name}.provision.#{node_id}") { |_, reply|
          @got_provision_request = true
          response = ProvisionResponse.new
          response.success = true
          response.credentials = {
              'name' => UUIDTools::UUID.random_create.to_s,
              'node_id' => node_id,
              'username' => UUIDTools::UUID.random_create.to_s,
              'password' => UUIDTools::UUID.random_create.to_s,
            }
          @nats.publish(reply, response.encode)
        }
        @nats.subscribe("#{service_name}.unprovision.#{node_id}") { |msg, reply|
          @got_unprovision_request = true
          response = SimpleResponse.new
          response.success = true
          @nats.publish(reply, response.encode)
        }
        @nats.subscribe("#{service_name}.bind.#{node_id}") { |msg, reply|
          @got_bind_request = true
          response = BindResponse.new
          response.success = true
          response.credentials = {
              'name' => UUIDTools::UUID.random_create.to_s,
              'node_id' => node_id,
              'username' => UUIDTools::UUID.random_create.to_s,
              'password' => UUIDTools::UUID.random_create.to_s,
            }
          @nats.publish(reply, response.encode)
        }
        @nats.subscribe("#{service_name}.unbind.#{node_id}") { |msg, reply|
          @got_unbind_request = true
          response = SimpleResponse.new
          response.success = true
          @nats.publish(reply, response.encode)
        }
        @nats.subscribe("#{service_name}.restore.#{node_id}") { |msg, reply|
          @got_restore_request = true
          response = SimpleResponse.new
          response.success = true
          @nats.publish(reply, response.encode)
        }
        @nats.subscribe("#{service_name}.check_orphan") do |msg|
          @got_check_orphan_request = true
          ins_list = Array.new(@id) { |i| (@id * 10 + i).to_s.ljust(36, "I") }
          bind_list = Array.new(@id) do |i|
            {
              "name" => (@id * 10 + i).to_s.ljust(36, "I"),
              "username" => (@id * 10 + i).to_s.ljust(18, "U"),
              "port" => i * 1000 + 1,
              "db" => "db#{@id}"
            }
          end
          request =  NodeHandlesReport.new
          request.instances_list = ins_list
          request.bindings_list = bind_list
          request.node_id = node_id
          @nats.publish("#{service_name}.node_handles", request.encode)
        end
        @nats.subscribe("#{service_name}.purge_orphan.#{node_id}") do |msg|
          @got_purge_orphan_request = true
          request = PurgeOrphanRequest.decode(msg)
          @purge_ins_list.concat(request.orphan_ins_list)
          @purge_bind_list.concat(request.orphan_binding_list)
        end
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
      a = { :id => node_id, :score => @score, :plan => @plan}
      @nats.publish(reply||"#{service_name}.announce", a.to_json)
    end
  end

  # The node that generates error response
  class MockErrorNode < MockNode
    include VCAP::Services::Base::Error
    attr_accessor :got_unprovision_request
    attr_accessor :got_provision_request
    attr_accessor :got_unbind_request
    attr_accessor :got_bind_request
    attr_accessor :got_restore_request
    def initialize(id, score)
      @id = id
      @plan = "free"
      @score = score
      @got_provision_request = false
      @got_unprovision_request = false
      @got_bind_request = false
      @got_unbind_request = false
      @got_restore_request = false
      @got_check_orphan_request = false
      @internal_error = ServiceError.new(ServiceError::INTERNAL_ERROR)
      @nats = NATS.connect(:uri => BaseTests::Options::NATS_URI) {
        @nats.subscribe("#{service_name}.discover") { |_, reply|
          announce(reply)
        }
        @nats.subscribe("#{service_name}.provision.#{node_id}") { |_, reply|
          @got_provision_request = true
          response = ProvisionResponse.new
          response.success = false
          response.error = @internal_error.to_hash
          @nats.publish(reply, response.encode)
        }
        @nats.subscribe("#{service_name}.unprovision.#{node_id}") { |msg, reply|
          @got_unprovision_request = true
          @nats.publish(reply, gen_simple_error_response.encode)
        }
        @nats.subscribe("#{service_name}.bind.#{node_id}") { |msg, reply|
          @got_bind_request = true
          response = BindResponse.new
          response.success = false
          response.error = @internal_error.to_hash
          @nats.publish(reply, response.encode)
        }
        @nats.subscribe("#{service_name}.unbind.#{node_id}") { |msg, reply|
          @got_unbind_request = true
          @nats.publish(reply, gen_simple_error_response.encode)
        }
        @nats.subscribe("#{service_name}.restore.#{node_id}") { |msg, reply|
          @got_restore_request = true
          @nats.publish(reply, gen_simple_error_response.encode)
        }
        @nats.subscribe("#{service_name}.check_orphan") do |msg|
          @got_check_orphan_request = true
          @nats.publish("#{service_name}.node_handles", "malformed node handles")
        end
        announce
      }
    end

    def gen_simple_error_response
      res = SimpleResponse.new
      res.success = false
      res.error = @internal_error.to_hash
      res
    end
  end

end
