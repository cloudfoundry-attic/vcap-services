# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require 'do'
require 'spec_helper'
require 'service_broker/async_gateway'


class ServiceBrokerHelper
  CC_PORT   = 34567
  GW_PORT   = 15000
  LOCALHOST = "127.0.0.1"

  def get_config
    config = load_config
    db_conf = config[:local_db]
    if db_conf.include?("/")
      dir = db_conf[db_conf.index(":")+1..db_conf.rindex("/")-1]
      FileUtils.mkdir_p(dir) unless File.exists?(dir)
    end

    config[:cloud_controller_uri] = "#{LOCALHOST}:#{CC_PORT}"
    config[:mbus] = "nats://nats:nats@#{VCAP.local_ip}:4222"
    config[:host] = LOCALHOST
    config[:port] = GW_PORT
    config[:url] = "http://#{LOCALHOST}:#{GW_PORT}"
    config[:node_timeout] = 1
    config[:cc_api_version] = "v1"

    config
  end

  def create_cc
    cc = MockCloudController.new
    cc.start
    cc
  end

  def create_service_broker_gateway
    gw = ServiceBrokerGateway.new(get_config)
    gw.start
    gw
  end

  def create_client
    ServiceBrokerClient.new(get_config)
  end

  class MockCloudController
    def initialize
      Thin::Logging.debug = true
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
        puts "\n*#*#*#*#* Registered #{svc["active"] == true ? "*ACTIVE*" : "*INACTIVE*"} offering: #{svc["label"]}\n\n"
        "{}"
      end

      get "/proxied_services/v1/offerings" do
        puts "*#*#*#*#* CC::GET(/proxied_services/v1/offerings): #{request.body.read}"
        Yajl::Encoder.encode({
          :proxied_services => @offerings.values
        })
      end
    end
  end

  class ServiceBrokerGateway

    def initialize(cfg)
      @config = cfg
      @gw = VCAP::Services::ServiceBroker::AsynchronousServiceGateway.new(@config)
      @server = Thin::Server.new(@config[:host], @config[:port], @gw)
    end

    def start
      Thread.new { @server.start }
    end

    def stop
      @server.stop
    end
  end

  class ServiceBrokerClient
    attr_accessor :last_http_code, :last_response

    def initialize(opts)
      @gw_host = opts[:host]
      @gw_port = opts[:port]
      @component_port = opts[:component_port]

      @token   = opts[:token]
      @content_type = 'application/json'
      @cc_head = {
        'Content-Type'         => @content_type,
        'X-VCAP-Service-Token' => @token,
      }
      @base_url = "http://#{@gw_host}:#{@gw_port}"
      @component_base_url = "http://#{@gw_host}:#{@component_port}"
    end

    def set_content_type(ct)
      old_content_type = @content_type
      @content_type = ct
      @cc_head['Content-Type'] = @content_type
      old_content_type
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
      puts "Received response: #{http.response_header.status}  - #{http.response.inspect}"
      @last_http_code = http.response_header.status
      @last_response = http.response
    end

    def send_get_request(url, body = nil)
      puts "Sending request to: #{@base_url}#{url}"
      http = EM::HttpRequest.new("#{@base_url}#{url}").get(gen_req(body))
      http.callback { set_last_result(http) }
      http.errback { set_last_result(http) }
    end

  end
end

module VCAP
  module Services
    module ServiceBroker
      class AsynchronousServiceGateway
        attr_reader :logger
      end
    end
  end
end


describe "Service Broker" do
  it "should return bad request if request type is not json " do
    EM.run do
      cc = nil
      sb = nil
      client = nil
      content_type = nil

      Do.at(0) {
        service_broker_helper = ServiceBrokerHelper.new
        cc = service_broker_helper.create_cc
        sb = service_broker_helper.create_service_broker_gateway
        client = service_broker_helper.create_client
        content_type = client.set_content_type('random')
      }

      Do.at(2) { client.send_get_request("/") }
      Do.at(3) {
        client.last_http_code.should == 400
        client.set_content_type(content_type)
      }

      Do.at(4) { sb.stop; cc.stop; EM.stop }
    end
  end

  it "should return unauthorize error with mismatch token " do
    EM.run do
      cc = nil
      sb = nil
      client = nil
      old_token = nil

      Do.at(0) {
        service_broker_helper = ServiceBrokerHelper.new
        cc = service_broker_helper.create_cc
        sb = service_broker_helper.create_service_broker_gateway
        client = service_broker_helper.create_client
        old_token = client.set_token("foobar")
      }

      Do.at(1) { client.send_get_request("/") }
      Do.at(2) {
        client.last_http_code.should == 401
        client.set_token(old_token)
      }

      Do.at(3) { sb.stop; cc.stop; EM.stop }
    end
  end

end
