require 'uuid'

class MarketplaceGatewayHelper
  CC_PORT = 34567

  GW_PORT = 15000
  GW_COMPONENT_PORT = 10000
  LOCALHOST = "127.0.0.1"

  def initialize
    @override_config = {}
  end

  def set(key, value)
    @override_config[key] = value
  end

  def get_config
    config = load_config("test")
    config[:cloud_controller_uri] = "#{LOCALHOST}:#{CC_PORT}"
    config[:mbus] = "nats://nats:nats@#{VCAP.local_ip}:4222"
    config[:host] = LOCALHOST
    config[:port] = GW_PORT
    config[:url] = "http://#{LOCALHOST}:#{GW_PORT}"
    config[:component_port] = GW_COMPONENT_PORT
    config[:user] = "u"
    config[:password] = "p"
    config[:node_timeout] = 1
    config[:cc_api_version] = "v1"

    @override_config.each { |k, v| config[k.to_sym] = v }

    config
  end

  def create_mpgw
    gw = Gateway.new(get_config)
    gw.start
    gw
  end

  def create_cc
    cc = MockCloudController.new
    cc.start
    cc
  end

  def create_ccng
    cc = MockCloudControllerNG.new
    cc.start
    cc
  end

  def create_client()
    config = get_config
    Client.new(config)
  end

  class MockCloudController
    def initialize
      @server = Thin::Server.new("#{LOCALHOST}", CC_PORT, Handler.new)
    end

    def start
      Thread.new { @server.start }
      while !@server.running?
        sleep 0.1
      end
    end

    def stop
      @server.stop if @server
    end

    class Handler < Sinatra::Base

      def initialize()
        @offerings = {}
      end

      post "/services/v1/offerings" do
        svc = JSON.parse(request.body.read)
        @offerings[svc["label"]] = svc
        "{}"
      end

      get "/proxied_services/v1/offerings" do
        Yajl::Encoder.encode({
          :proxied_services => @offerings.values
        })
      end
    end
  end

  class MockCloudControllerNG
    def initialize
      @server = Thin::Server.new("#{LOCALHOST}", CC_PORT, Handler.new)
    end

    def start
      Thread.new { @server.start }
      while !@server.running?
        sleep 0.1
      end
    end

    def stop
      @server.stop if @server
    end

    class Handler < Sinatra::Base

      def initialize()
        @offerings = {}
        @offering_plans = {}
      end

      post "/v2/services" do
        svc_uuid = UUIDTools::UUID.random_create.to_s
        offering = {
          "metadata" => { "guid" => svc_uuid, "url" => "/v2/services/#{svc_uuid}", "created_at" => Time.now.to_s, "updated_at" => nil },
          "entity" => JSON.parse(request.body.read)
        }
        offering["entity"]["service_plans_url"] = "/v2/services/#{svc_uuid}/service_plans"
        @offerings[svc_uuid] = offering

        Yajl::Encoder.encode(offering)
      end

      post "/v2/service_plans" do
        svc_plan_uuid = UUIDTools::UUID.random_create.to_s
        svc_plan = {
          "metadata" => { "guid" => svc_plan_uuid, "url" => "/v2/service_plans/#{svc_plan_uuid}", "created_at" => Time.now.to_s, "updated_at" => nil },
          "entity" => JSON.parse(request.body.read)
        }
        svc_plan["entity"]["service_instance_guids"] = []
        svc_plan["entity"]["service_instances_url"] = "/v2/service_plans/#{svc_plan_uuid}/service_instance"
        svc_plan["entity"]["service_url"] = "/v2/services/#{svc_plan["entity"]["service_guid"]}"


        svc_uuid = svc_plan["entity"]["service_guid"]

        @offerings[svc_uuid]["entity"]["service_plans"] ||= []
        @offerings[svc_uuid]["entity"]["service_plans"] << svc_plan

        @offering_plans[svc_plan_uuid] = svc_plan

        Yajl::Encoder.encode(svc_plan)
      end

      get "/v2/services" do
        Yajl::Encoder.encode({
          "total_results" => @offerings.size,
          "total_pages"   => 1,
          "prev_url"      => nil,
          "next_url"      => nil,
          "resources"     => @offerings.values
        })
      end

      get "/v2/services/:service_guid/service_plans" do
        entries = @offerings[params[:service_guid]]["entity"]["service_plans"]

        Yajl::Encoder.encode({
          "total_results" => entries.size,
          "total_pages"   => 1,
          "prev_url"      => nil,
          "next_url"      => nil,
          "resources"     => entries
        })
      end
    end
  end

  class Gateway

    def initialize(cfg)
      @config = cfg
      @mpgw = VCAP::Services::Marketplace::MarketplaceServiceGateway.new(@config.merge(logger: Logger.new('/dev/null')))
      @server = Thin::Server.new(@config[:host], @config[:port], @mpgw)
    end

    def start
      Thread.new { @server.start }
    end

    def stop
      @server.stop
    end
  end

  class Client

    attr_accessor :last_http_code, :last_response

    def initialize(opts)
      @gw_host = opts[:host]
      @gw_port = opts[:port]
      @component_port = opts[:component_port]
      @credentials = [ opts[:user], opts[:password] ]

      @token   = opts[:token]
      @cc_head = {
        'Content-Type' => 'application/json',
        'X-VCAP-Service-Token' => @token,
      }
      @base_url = "http://#{@gw_host}:#{@gw_port}"
      @component_base_url = "http://#{@gw_host}:#{@component_port}"
    end

    def set_token(tok)
      old_token = @token
      @token = tok
      @cc_head['X-VCAP-Service-Token'] = @token
      old_token
    end

    def gen_req(body = nil)
      req = {}
      req[:head] = @cc_head
      req[:body] = body if body
      req
    end

    def set_last_result(http)
      @last_http_code = http.response_header.status
      @last_response = http.response
    end

    def get_varz
      http = EM::HttpRequest.new("#{@component_base_url}/varz").get :head => {'authorization' => @credentials}
      http.callback { set_last_result(http) }
      http.errback { set_last_result(http) }
    end

    def get_healthz
      http = EM::HttpRequest.new("#{@component_base_url}/healthz").get :head => {'authorization' => @credentials}
      http.callback { set_last_result(http) }
      http.errback { set_last_result(http) }
    end

    def send_get_request(url, body = nil)
      http = EM::HttpRequest.new("#{@base_url}#{url}").get(gen_req(body))
      http.callback { set_last_result(http) }
      http.errback { set_last_result(http) }
    end

    def set_config(key, value)
      url = "#{@base_url}/marketplace/set/#{key}/#{value}"
      http = EM::HttpRequest.new("#{url}").post(gen_req)
      http.callback { set_last_result(http) }
      http.errback { set_last_result(http) }
    end

    def send_provision_request(label, name, email, plan, version)
      msg = VCAP::Services::Api::GatewayProvisionRequest.new(
        :label => label,
        :name =>  name,
        :email => email,
        :plan =>  plan,
        :version => version
      ).encode
      http = EM::HttpRequest.new("http://#{@gw_host}:#{@gw_port}/gateway/v1/configurations").post(gen_req(msg))
      http.callback { set_last_result(http) }
      http.errback { set_last_result(http) }
    end

    def send_unprovision_request(service_id)
      raise "Null service id" if service_id.nil?
      http = EM::HttpRequest.new("http://#{@gw_host}:#{@gw_port}/gateway/v1/configurations/#{service_id}").delete(gen_req)
      http.callback { set_last_result(http) }
      http.errback { set_last_result(http) }
    end

    def send_bind_request(service_id, label, email, opts)
      raise "Null service id" if service_id.nil?
      msg = VCAP::Services::Api::GatewayBindRequest.new(
        :service_id => service_id,
        :label => label,
        :email => email,
        :binding_options => opts
      ).encode

      http = EM::HttpRequest.new("http://#{@gw_host}:#{@gw_port}/gateway/v1/configurations/#{service_id}/handles").post(gen_req(msg))
      http.callback { set_last_result(http) }
      http.errback { set_last_result(http) }
    end

    def send_unbind_request(service_id, bind_id)
      raise "Null service id" if service_id.nil?
      raise "Null bind id" if bind_id.nil?
      msg = Yajl::Encoder.encode({
        :service_id => service_id,
        :handle_id => bind_id,
        :binding_options => {}
      })
      http = EM::HttpRequest.new("http://#{@gw_host}:#{@gw_port}/gateway/v1/configurations/#{service_id}/handles/#{bind_id}").delete(gen_req(msg))
      http.callback { set_last_result(http) }
      http.errback { set_last_result(http) }
    end
  end
end
