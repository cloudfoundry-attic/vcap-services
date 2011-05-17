# Copyright (c) 2009-2011 VMware, Inc.
# XXX(mjp)
$:.unshift(File.expand_path("../../../lib", __FILE__))
require 'rubygems'

require 'eventmachine'
require 'em-http-request'
require 'json'
require 'sinatra/base'
require 'uri'
require 'thread'

require 'json_message'
require 'services/api'

$:.unshift(File.expand_path("../../base/lib", __FILE__))
require 'base/service_error'

module VCAP
  module Services
  end
end

# A simple service gateway that proxies requests onto an asynchronous service provisioners.
# NB: Do not use this with synchronous provisioners, it will produce unexpected results.
#
# TODO(mjp): This needs to handle unknown routes
class VCAP::Services::AsynchronousServiceGateway < Sinatra::Base

  include VCAP::Services::Base::Error

  REQ_OPTS            = %w(service token provisioner cloud_controller_uri).map {|o| o.to_sym}

  # Allow our exception handlers to take over
  set :raise_errors, Proc.new {false}
  set :show_exceptions, false

  def initialize(opts)
    super
    missing_opts = REQ_OPTS.select {|o| !opts.has_key? o}
    raise ArgumentError, "Missing options: #{missing_opts.join(', ')}" unless missing_opts.empty?
    @service      = opts[:service]
    @token        = opts[:token]
    @logger       = opts[:logger] || make_logger()
    @cld_ctrl_uri = http_uri(opts[:cloud_controller_uri])
    @offering_uri = "#{@cld_ctrl_uri}/services/v1/offerings"
    @provisioner  = opts[:provisioner]
    @hb_interval  = opts[:heartbeat_interval] || 60
    @handles_uri = "#{@cld_ctrl_uri}/services/v1/offerings/#{@service[:label]}/handles"
    @handle_fetch_interval = opts[:handle_fetch_interval] || 60
    @svc_json     = {
      :label  => @service[:label],
      :url    => @service[:url],
      :plans  => @service[:plans],
      :tags   => @service[:tags],
      :active => true,
      :description  => @service[:description],
      :plan_options => @service[:plan_options],
      :acls => @service[:acls]
    }.to_json
    @deact_json   = {
      :label  => @service[:label],
      :url    => @service[:url],
      :active => false
    }.to_json
    @cc_req_hdrs  = {
      'Content-Type'         => 'application/json',
      'X-VCAP-Service-Token' => @token,
    }

    # Setup heartbeats and exit handlers
    EM.add_periodic_timer(@hb_interval) { send_heartbeat }
    EM.next_tick { send_heartbeat }
    Kernel.at_exit do
      if EM.reactor_running?
        # :/ We can't stop others from killing the event-loop here. Let's hope that they play nice
        send_deactivation_notice(false)
      else
        EM.run { send_deactivation_notice }
      end
    end

    # Add any necessary handles we don't know about
    update_callback = Proc.new do |resp|
      @provisioner.update_handles(resp.handles)
      EM.cancel_timer(@fetch_handle_timer)
    end
    @fetch_handle_timer = EM.add_periodic_timer(@handle_fetch_interval) { fetch_handles(&update_callback) }
    EM.next_tick { fetch_handles(&update_callback) }

    # Register update handle callback
    @provisioner.register_update_handle_callback{|handle, &blk| update_service_handle(handle, &blk)}
  end


  # Validate the incoming request
  before do
    unless request.media_type == Rack::Mime.mime_type('.json')
      error_msg = ServiceError.new(ServiceError::INVALID_CONTENT).to_hash
      abort_request(error_msg)
    end
    unless auth_token && (auth_token == @token)
      error_msg = ServiceError.new(ServiceError::NOT_AUTHORIZED).to_hash
      abort_request(error_msg)
    end
    content_type :json
  end

  # Handle errors that result from malformed requests
  error [JsonMessage::ValidationError, JsonMessage::ParseError] do
    error_msg = ServiceError.new(ServiceError::MALFORMATTED_REQ).to_hash
    abort_request(error_msg)
  end

  #################### Handlers ####################

  # Provisions an instance of the service
  #
  post '/gateway/v1/configurations' do
    req = Yajl::Parser.parse(request_body)
    @logger.debug("Provision request for label=#{req['label']} plan=#{req['plan']}")

    name, version = VCAP::Services::Api::Util.parse_label(req['label'])
    unless (name == @service[:name]) && (version == @service[:version])
      error_msg = ServiceError.new(ServiceError::UNKNOWN_LABEL).to_hash
      abort_request(error_msg)
    end

    @provisioner.provision_service(req) do |msg|
      if msg['success']
        async_reply(VCAP::Services::Api::ProvisionResponse.new(msg['response']).encode)
      else
        async_reply_error(msg['response'])
      end
    end
    async_mode
  end

  # Unprovisions a previously provisioned instance of the service
  #
  delete '/gateway/v1/configurations/:service_id' do
    @logger.debug("Unprovision request for service_id=#{params['service_id']}")

    @provisioner.unprovision_service(params['service_id']) do |msg|
      if msg['success']
        async_reply
      else
        async_reply_error(msg['response'])
      end
    end
    async_mode
  end

  # Binds a previously provisioned instance of the service to an application
  #
  post '/gateway/v1/configurations/:service_id/handles' do
    @logger.info("Binding request for service=#{params['service_id']}")

    req = VCAP::Services::Api::GatewayBindRequest.decode(request_body)
    @logger.debug("Binding options: #{req.binding_options.inspect}")

    @provisioner.bind_instance(req.service_id, req.binding_options) do |msg|
      if msg['success']
        async_reply(VCAP::Services::Api::GatewayBindResponse.new(msg['response']).encode)
      else
        async_reply_error(msg['response'])
      end
    end
    async_mode
  end

  # Unbinds a previously bound instance of the service
  #
  delete '/gateway/v1/configurations/:service_id/handles/:handle_id' do
    @logger.info("Unbind request for service_id=#{params['service_id']} handle_id=#{params['handle_id']}")

    req = VCAP::Services::Api::GatewayUnbindRequest.decode(request_body)

    @provisioner.unbind_instance(req.service_id, req.handle_id, req.binding_options) do |msg|
      if msg['success']
        async_reply
      else
        async_reply_error(msg['response'])
      end
    end
    async_mode
  end

  # Restore an instance of the service
  #
  post '/service/internal/v1/restore' do
    @logger.info("Restore service")

    req = Yajl::Parser.parse(request_body)
    # TODO add json format check

    @provisioner.restore_instance(req['instance_id'], req['backup_path']) do |msg|
      if msg['success']
        async_reply
      else
        async_reply_error(msg['response'])
      end
    end
    async_mode
  end

  # Recovery an instance if node is crashed.
  post '/service/internal/v1/recover' do
    @logger.info("Recover service request.")
    request = Yajl::Parser.parse(request_body)
    instance_id = request['instance_id']
    backup_path = request['backup_path']
    fetch_handles do |resp|
      @provisioner.recover(instance_id, backup_path, resp.handles) do |msg|
        if msg['success']
          async_reply
        else
          async_reply_error(msg['response'])
        end
      end
    end
    async_mode
  end


  #################### Helpers ####################

  helpers do

    # Aborts the request with the supplied errs
    #
    # +errs+  Hash of section => err
    def abort_request(error_msg)
      err_body = error_msg['msg'].to_json()
      halt(error_msg['status'], {'Content-Type' => Rack::Mime.mime_type('.json')}, err_body)
    end

    def auth_token
      @auth_token ||= request_header(VCAP::Services::Api::GATEWAY_TOKEN_HEADER)
      @auth_token
    end

    def request_body
      request.body.rewind
      request.body.read
    end

    def request_header(header)
      # This is pretty ghetto but Rack munges headers, so we need to munge them as well
      rack_hdr = "HTTP_" + header.upcase.gsub(/-/, '_')
      env[rack_hdr]
    end

    # Update a service handle using REST
    def update_service_handle(handle, &cb)
      @logger.debug("Update service handle: #{handle.inspect}")
      if not handle
        cb.call(false) if cb
        return
      end
      id = handle["service_id"]
      uri = @handles_uri + "/#{id}"
      handle_json = Yajl::Encoder.encode(handle)
      req = {
        :head => @cc_req_hdrs,
        :body => handle_json,
      }
      http = EM::HttpRequest.new(uri).post(req)
      http.callback do
        if http.response_header.status == 200
          @logger.info("Successful update handle #{id}.")
          # Update local array in provisioner
          @provisioner.update_handles([handle])
          cb.call(true) if cb
        else
          @logger.error("Failed to update handle #{id}: #{http.error}")
          cb.call(false) if cb
        end
      end
      http.errback do
        @logger.error("Failed to update handle #{id}: #{http.error}")
        cb.call(false) if cb
      end
    end

    def async_mode(timeout=10)
      request.env['__async_timer'] = EM.add_timer(timeout) do
        @logger.warn("Service Unavailable")
        error_msg = ServiceError.new(ServiceError::SERVICE_UNAVAILABLE).to_hash
        err_body = error_msg['msg'].to_json()
        request.env['async.callback'].call(
          [
            error_msg['status'],
            {'Content-Type' => Rack::Mime.mime_type('.json')},
            err_body
          ]
        )
      end unless request.env['done'] ||= false
      throw :async
    end

    def async_reply(resp='{}')
      async_reply_raw(200, {'Content-Type' => Rack::Mime.mime_type('.json')}, resp)
    end

    def async_reply_raw(status, headers, body)
      @logger.debug("Reply status:#{status}, headers:#{headers}, body:#{body}")
      request.env['done'] = true
      EM.cancel_timer(request.env['__async_timer']) if request.env['__async_timer']
      request.env['async.callback'].call([status, headers, body])
    end

    def async_reply_error(error_msg)
      err_body = error_msg['msg'].to_json()
      async_reply_raw(error_msg['status'], {'Content-Type' => Rack::Mime.mime_type('.json')}, err_body)
    end
  end

  private

  def make_logger()
    logger = Logger.new(STDOUT)
    logger.level = Logger::DEBUG
    logger
  end

  # Lets the cloud controller know we're alive and where it can find us
  def send_heartbeat
    @logger.info("Sending info to cloud controller: #{@offering_uri}")

    req = {
      :head => @cc_req_hdrs,
      :body => @svc_json,
    }
    http = EM::HttpRequest.new(@offering_uri).post(req)

    http.callback do
      if http.response_header.status == 200
        @logger.info("Successfully registered with cloud controller")
      else
        @logger.error("Failed registering with cloud controller, status=#{http.response_header.status}")
      end
    end

    http.errback do
      @logger.error("Failed registering with cloud controller: #{http.error}")
    end
  end

  # Lets the cloud controller know that we're going away
  def send_deactivation_notice(stop_event_loop=true)
    @logger.info("Sending deactivation notice to cloud controller: #{@offering_uri}")

    req = {
      :head => @cc_req_hdrs,
      :body => @deact_json,
    }
    http = EM::HttpRequest.new(@offering_uri).post(req)

    http.callback do
      if http.response_header.status == 200
        @logger.info("Successfully deactivated with cloud controller")
      else
        @logger.error("Failed deactivation with cloud controller, status=#{http.response_header.status}")
      end
      EM.stop if stop_event_loop
    end

    http.errback do
      @logger.error("Failed deactivation with cloud controller: #{http.error}")
      EM.stop if stop_event_loop
    end
  end

  # Fetches canonical state (handles) from the Cloud Controller
  def fetch_handles(&cb)
    @logger.info("Fetching handles from cloud controller @ #{@handles_uri}")

    req = {
      :head => @cc_req_hdrs,
    }
    http = EM::HttpRequest.new(@handles_uri).get(req)

    http.callback do
      if http.response_header.status == 200
        @logger.info("Successfully fetched handles")
        begin
          resp = VCAP::Services::Api::ListHandlesResponse.decode(http.response)
        rescue => e
          @logger.error("Error decoding reply from gateway:")
          @logger.error("#{e}")
          next
        end
        cb.call(resp)
      else
        @logger.error("Failed fetching handles, status=#{http.response_header.status}")
      end
    end

    http.errback do
      @logger.error("Failed fetching handles: #{http.error}")
    end
  end

  def http_uri(uri)
    uri = "http://#{uri}" if (uri.index('http://') != 0)
    uri
  end
end
