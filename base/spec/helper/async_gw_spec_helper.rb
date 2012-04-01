require 'base/asynchronous_service_gateway'

class AsyncGatewayTests
  CC_PORT = 34512
  GW_PORT = 34513
  NODE_TIMEOUT = 5

  def self.create_nice_gateway
    MockGateway.new(true)
  end

  def self.create_nasty_gateway
    MockGateway.new(false)
  end

  def self.create_check_orphan_gateway(nice, check_interval, double_check_interval)
    MockGateway.new(nice, nil, check_interval, double_check_interval)
  end

  def self.create_timeout_gateway(nice, timeout)
    MockGateway.new(nice, timeout)
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
    attr_reader   :migrate_http_code
    attr_reader   :instances_http_code
    attr_reader   :purge_orphan_http_code
    attr_reader   :check_orphan_http_code

    def initialize(nice, timeout=nil, check_interval=-1, double_check_interval=3)
      @token = '0xdeadbeef'
      @cc_head = {
        'Content-Type'         => 'application/json',
        'X-VCAP-Service-Token' => @token,
      }
      @label = "service-1.0"
      if timeout
        # Nice timeout provisioner will finish the job in timeout,
        # while un-nice timeout provisioner won't.
        @sp = nice ?
          TimeoutProvisioner.new(timeout - 1) :
          TimeoutProvisioner.new(timeout + 1)
      else
        @sp = nice ? NiceProvisioner.new : NastyProvisioner.new
      end
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
        :provisioner => @sp,
        :node_timeout => timeout || NODE_TIMEOUT,
        :cloud_controller_uri => "http://localhost:#{CC_PORT}",
        :check_orphan_interval => check_interval,
        :double_check_orphan_interval => double_check_interval
      )
      @server = Thin::Server.new('localhost', GW_PORT, sg)
      @provision_http_code = 0
      @unprovision_http_code = 0
      @bind_http_code = 0
      @unbind_http_code = 0
      @restore_http_code = 0
      @recover_http_code = 0
      @migrate_http_code = 0
      @instances_http_code = 0
      @purge_orphan_http_code = 0
      @check_orphan_http_code = 0
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

    def check_orphan_invoked
      @sp.check_orphan_invoked
    end

    def double_check_orphan_invoked
      @sp.double_check_orphan_invoked
    end

    def send_provision_request
      msg = VCAP::Services::Api::GatewayProvisionRequest.new(
        :label => @label,
        :name  => 'service',
        :email => "foobar@abc.com",
        :plan  => "free"
      ).encode
      http = EM::HttpRequest.new("http://localhost:#{GW_PORT}/gateway/v1/configurations").post(gen_req(msg))
      http.callback {
        @provision_http_code = http.response_header.status
        if @provision_http_code == 200
          res = VCAP::Services::Api::GatewayProvisionResponse.decode(http.response)
          @last_service_id = res.service_id
        end
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
      msg = VCAP::Services::Api::GatewayBindRequest.new(
        :service_id => service_id,
        :label => @label,
        :email => "foobar@abc.com",
        :binding_options => {}
      ).encode
      http = EM::HttpRequest.new("http://localhost:#{GW_PORT}/gateway/v1/configurations/#{service_id}/handles").post(gen_req(msg))
      http.callback {
        @bind_http_code = http.response_header.status
        if @bind_http_code == 200
          res = VCAP::Services::Api::GatewayBindResponse.decode(http.response)
          @last_bind_id = res.service_id
        end
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

    def send_migrate_request(service_id = nil)
      http = EM::HttpRequest.new("http://localhost:#{GW_PORT}/service/internal/v1/migration/test_node/test_instance/test_action").post(gen_req)
      http.callback {
        @migrate_http_code = http.response_header.status
      }
      http.errback {
        @migrate_http_code = -1
      }
    end

    def send_instances_request(service_id = nil)
      http = EM::HttpRequest.new("http://localhost:#{GW_PORT}/service/internal/v1/migration/test_node/instances").get(gen_req)
      http.callback {
        @instances_http_code = http.response_header.status
      }
      http.errback {
        @instances_http_code = -1
      }
    end

    def send_purge_orphan_request
      msg = Yajl::Encoder.encode({
        :orphan_instances => TEST_PURGE_INS_HASH,
        :orphan_bindings => TEST_PURGE_BIND_HASH
      })
      http = EM::HttpRequest.new("http://localhost:#{GW_PORT}/service/internal/v1/purge_orphan").delete(gen_req(msg))
      http.callback {
        @purge_orphan_http_code = http.response_header.status
      }
      http.errback {
        @purge_orphan_http_code = -1
      }
    end
    def send_check_orphan_request
      msg = Yajl::Encoder.encode({
      })
      http = EM::HttpRequest.new("http://localhost:#{GW_PORT}/service/internal/v1/check_orphan").post(gen_req(msg))
      http.callback {
        @check_orphan_http_code = http.response_header.status
      }
      http.errback {
        @check_orphan_http_code = -1
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
    attr_accessor :got_migrate_request
    attr_accessor :got_instances_request
    attr_reader   :purge_orphan_invoked
    attr_reader   :check_orphan_invoked
    attr_reader   :double_check_orphan_invoked

    def initialize
      @got_provision_request = false
      @got_unprovision_request = false
      @got_bind_request = false
      @got_unbind_request = false
      @got_restore_request = false
      @got_recover_request = false
      @got_migrate_request = false
      @got_instances_request = false
      @purge_orphan_invoked = false
      @check_orphan_invoked = false
      @double_check_orphan_invoked = false
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
      @got_recover_request = true
      blk.call(success(true))
    end

    def migrate_instance(node_id, instance_id, action, &blk)
      @got_migrate_request = true
      blk.call(success(true))
    end

    def get_instance_id_list(node_id, &blk)
      @got_instances_request = true
      blk.call(success(true))
    end

    def purge_orphan(orphan_ins_hash, orphan_binding_hash, &blk)
      @purge_orphan_invoked = true
      blk.call(success(true))
    end

    def check_orphan(handles, &blk)
      @check_orphan_invoked = true
      blk.call(success(true))
    end

    def double_check_orphan(handles)
      @double_check_orphan_invoked = true
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
      @got_recover_request = true
      blk.call(internal_fail)
    end

    def migrate_instance(node_id, instance_id, action, &blk)
      @got_migrate_request = true
      blk.call(internal_fail)
    end

    def get_instance_id_list(node_id, &blk)
      @got_instances_request = true
      blk.call(internal_fail)
    end

    def purge_orphan(orphan_ins_hash,orphan_binding_hash,&blk)
      @purge_orphan_invoked = true
      blk.call(internal_fail)
    end
    def check_orphan(handles,&blk)
      @check_orphan_invoked = true
      blk.call(internal_fail)
    end
  end

  # Timeout Provisioner is a simple version of provisioner.
  # It only support provisioning.
  class TimeoutProvisioner < MockProvisioner
    def initialize(timeout)
      @timeout = timeout
    end

    def provision_service(request, prov_handle=nil, &blk)
      @got_provision_request = true
      EM.add_timer(@timeout) do
        blk.call(
          success({
            :data => {},
            :service_id => SERV_ID,
            :credentials => {}
            }
          )
        )
      end
    end
  end
end
