# Copyright (c) 2009-2011 VMware, Inc.
# XXX(mjp)
require 'rubygems'

require 'eventmachine'
require 'em-http-request'
require 'json'
require 'sinatra/base'
require 'uri'
require 'thread'
require 'json_message'
require 'services/api'
require 'services/api/const'

$:.unshift(File.dirname(__FILE__))
require 'service_error'

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
    setup(opts)
  end

  # setup the environment
  def setup(opts)
    missing_opts = REQ_OPTS.select {|o| !opts.has_key? o}
    raise ArgumentError, "Missing options: #{missing_opts.join(', ')}" unless missing_opts.empty?
    @service      = opts[:service]
    @token        = opts[:token]
    @logger       = opts[:logger] || make_logger()
    @cld_ctrl_uri = http_uri(opts[:cloud_controller_uri])
    @offering_uri = "#{@cld_ctrl_uri}/services/v1/offerings"
    @provisioner  = opts[:provisioner]
    @hb_interval  = opts[:heartbeat_interval] || 60
    @node_timeout = opts[:node_timeout]
    @handles_uri = "#{@cld_ctrl_uri}/services/v1/offerings/#{@service[:label]}/handles"
    @handle_fetch_interval = opts[:handle_fetch_interval] || 1
    @check_orphan_interval = opts[:check_orphan_interval] || -1
    @double_check_orphan_interval = opts[:double_check_orphan_interval] || 300
    @handle_fetched = false
    @fetching_handles = false
    @api_extensions = opts[:api_extensions] || []
    @svc_json     = {
      :label  => @service[:label],
      :url    => @service[:url],
      :plans  => @service[:plans],
      :tags   => @service[:tags],
      :active => true,
      :description  => @service[:description],
      :plan_options => @service[:plan_options],
      :acls => @service[:acls],
      :timeout => @service[:timeout],
    }.to_json

    @deact_json   = {
      :label  => @service[:label],
      :url    => @service[:url],
      :active => false
    }.to_json

    token_hdrs = VCAP::Services::Api::GATEWAY_TOKEN_HEADER
    @cc_req_hdrs  = {
      'Content-Type' => 'application/json',
      token_hdrs     => @token,
    }
    @proxy_opts = opts[:proxy]

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
      @handle_fetched = true
      EM.cancel_timer(@fetch_handle_timer)
    end
    @fetch_handle_timer = EM.add_periodic_timer(@handle_fetch_interval) { fetch_handles(&update_callback) }
    EM.next_tick { fetch_handles(&update_callback) }

    if @check_orphan_interval > 0
      handler_check_orphan = Proc.new do |resp|
        check_orphan(resp.handles,
                     lambda { @logger.info("Check orphan is requested") },
                     lambda { |errmsg| @logger.error("Error on requesting to check orphan #{errmsg}") })
      end
      EM.add_periodic_timer(@check_orphan_interval) { fetch_handles(&handler_check_orphan) }
    end

    # Register update handle callback
    @provisioner.register_update_handle_callback{|handle, &blk| update_service_handle(handle, &blk)}
  end

  def check_orphan(handles, callback, errback)
    @provisioner.check_orphan(handles) do |msg|
      if msg['success']
        callback.call
        EM.add_timer(@double_check_orphan_interval) { fetch_handles{ |rs| @provisioner.double_check_orphan(rs.handles) } }
      else
        errback.call(msg['response'])
      end
    end
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
    unless @handle_fetched
      error_msg = ServiceError.new(ServiceError::SERVICE_UNAVAILABLE).to_hash
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
    req = VCAP::Services::Api::GatewayProvisionRequest.decode(request_body)
    @logger.debug("Provision request for label=#{req.label} plan=#{req.plan}")

    name, version = VCAP::Services::Api::Util.parse_label(req.label)
    unless (name == @service[:name]) && (version == @service[:version])
      error_msg = ServiceError.new(ServiceError::UNKNOWN_LABEL).to_hash
      abort_request(error_msg)
    end

    @provisioner.provision_service(req) do |msg|
      if msg['success']
        async_reply(VCAP::Services::Api::GatewayProvisionResponse.new(msg['response']).encode)
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

  # create a snapshot
  post "/gateway/v1/configurations/:service_id/snapshots" do
    not_impl unless @api_extensions.include? "snapshots"
    service_id = params["service_id"]
    @logger.info("Create snapshot request for service_id=#{service_id}")
    @provisioner.create_snapshot(service_id) do |msg|
      if msg['success']
        async_reply(VCAP::Services::Api::Job.new(msg['response']).encode)
      else
        async_reply_error(msg['response'])
      end
    end
    async_mode
  end

  # Get snapshot details
  get "/gateway/v1/configurations/:service_id/snapshots/:snapshot_id" do
    not_impl unless @api_extensions.include? "snapshots"
    service_id = params["service_id"]
    snapshot_id = params["snapshot_id"]
    @logger.info("Get snapshot_id=#{snapshot_id} request for service_id=#{service_id}")
    @provisioner.get_snapshot(service_id, snapshot_id) do |msg|
      if msg['success']
        async_reply(VCAP::Services::Api::Snapshot.new(msg['response']).encode)
      else
        async_reply_error(msg['response'])
      end
    end
    async_mode
  end

  # Enumreate snapshot
  get "/gateway/v1/configurations/:service_id/snapshots" do
    not_impl unless @api_extensions.include? "snapshots"
    service_id = params["service_id"]
    @logger.info("Enumerate snapshots request for service_id=#{service_id}")
    @provisioner.enumerate_snapshots(service_id) do |msg|
      if msg['success']
        async_reply(VCAP::Services::Api::SnapshotList.new(msg['response']).encode)
      else
        async_reply_error(msg['response'])
      end
    end
    async_mode
  end

  # Rollback to a snapshot
  put "/gateway/v1/configurations/:service_id/snapshots/:snapshot_id" do
    not_impl unless @api_extensions.include? "snapshots"
    service_id = params["service_id"]
    snapshot_id = params["snapshot_id"]
    @logger.info("Rollback service_id=#{service_id} to snapshot_id=#{snapshot_id}")
    @provisioner.rollback_snapshot(service_id, snapshot_id) do |msg|
      if msg['success']
        async_reply(VCAP::Services::Api::Job.new(msg['response']).encode)
      else
        async_reply_error(msg['response'])
      end
    end
    async_mode
  end

  # Delete a snapshot
  delete "/gateway/v1/configurations/:service_id/snapshots/:snapshot_id" do
    not_impl unless @api_extensions.include? "snapshots"
    service_id = params["service_id"]
    snapshot_id = params["snapshot_id"]
    @logger.info("Delete service_id=#{service_id} to snapshot_id=#{snapshot_id}")
    @provisioner.delete_snapshot(service_id, snapshot_id) do |msg|
      if msg['success']
        async_reply(VCAP::Services::Api::Job.new(msg['response']).encode)
      else
        async_reply_error(msg['response'])
      end
    end
    async_mode
  end

  # Get serialized url
  get "/gateway/v1/configurations/:service_id/serialized/url" do
    not_impl unless @api_extensions.include? "serialization"
    service_id = params["service_id"]
    @logger.info("Get serialized url for service_id=#{service_id}")
    @provisioner.get_serialized_url(service_id) do |msg|
      if msg['success']
        async_reply(VCAP::Services::Api::Job.new(msg['response']).encode)
      else
        async_reply_error(msg['response'])
      end
    end
    async_mode
  end


  # Import serialized data from url
  put "/gateway/v1/configurations/:service_id/serialized/url" do
    not_impl unless @api_extensions.include? "serialization"
    req = VCAP::Services::Api::SerializedURL.decode(request_body)
    service_id = params["service_id"]
    @logger.info("Import serialized data from url:#{req.url} for service_id=#{service_id}")
    @provisioner.import_from_url(service_id, req.url) do |msg|
      if msg['success']
        async_reply(VCAP::Services::Api::Job.new(msg['response']).encode)
      else
        async_reply_error(msg['response'])
      end
    end
    async_mode
  end

  # Import serialized data from request
  put "/gateway/v1/configurations/:service_id/serialized/data" do
    not_impl unless @api_extensions.include? "serialization"
    req = VCAP::Services::Api::SerializedData.decode(request_body)
    service_id = params["service_id"]
    @logger.info("Import data from request for service_id=#{service_id}")
    @provisioner.import_from_data(service_id, req) do |msg|
      if msg['success']
        async_reply(VCAP::Services::Api::Job.new(msg['response']).encode)
      else
        async_reply_error(msg['response'])
      end
    end
    async_mode
  end

  # Get Job details
  get "/gateway/v1/configurations/:service_id/jobs/:job_id" do
    not_impl unless @api_extensions.include? "jobs"
    service_id = params["service_id"]
    job_id = params["job_id"]
    @logger.info("Get job=#{job_id} for service_id=#{service_id}")
    @provisioner.job_details(service_id, job_id) do |msg|
      if msg['success']
        async_reply(VCAP::Services::Api::Job.new(msg['response']).encode)
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

  post '/service/internal/v1/check_orphan' do
    @logger.info("Request to check orphan")
    fetch_handles do |resp|
      check_orphan(resp.handles,
                   lambda { async_reply },
                   lambda { |errmsg| async_reply_error(errmsg) })
    end
    async_mode
  end

  delete '/service/internal/v1/purge_orphan' do
    @logger.info("Purge orphan request")
    req = Yajl::Parser.parse(request_body)
    orphan_ins_hash = req["orphan_instances"]
    orphan_binding_hash = req["orphan_bindings"]
    @provisioner.purge_orphan(orphan_ins_hash,orphan_binding_hash) do |msg|
      if msg['success']
        async_reply
      else
        async_reply_error(msg['response'])
      end
    end
    async_mode
  end

  # Service migration API
  post "/service/internal/v1/migration/:node_id/:instance_id/:action" do
    @logger.info("Migration: #{params["action"]} instance #{params["instance_id"]} in #{params["node_id"]}")
    @provisioner.migrate_instance(params["node_id"], params["instance_id"], params["action"]) do |msg|
      if msg["success"]
        async_reply(msg["response"].to_json)
      else
        async_reply_error(msg["response"])
      end
    end
    async_mode
  end

  get "/service/internal/v1/migration/:node_id/instances" do
    @logger.info("Migration: get instance id list of node #{params["node_id"]}")
    @provisioner.get_instance_id_list(params["node_id"]) do |msg|
      if msg["success"]
        async_reply(msg["response"].to_json)
      else
        async_reply_error(msg["response"])
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
          @logger.error("Failed to update handle #{id}: http status #{http.response_header.status}, error: #{http.error}")
          cb.call(false) if cb
        end
      end
      http.errback do
        @logger.error("Failed to update handle #{id}: #{http.error}")
        cb.call(false) if cb
      end
    end

    def async_mode(timeout=@node_timeout)
      request.env['__async_timer'] = EM.add_timer(timeout) do
        @logger.warn("Request timeout in #{timeout} seconds.")
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

    def not_impl
      halt 501, {'Content-Type' => Rack::Mime.mime_type('.json') }
    end
  end

  private

  def add_proxy_opts(req)
    req[:proxy] = @proxy_opts
    # this is a workaround for em-http-requesr 0.3.0 so that headers are not lost
    # more info: https://github.com/igrigorik/em-http-request/issues/130
    req[:proxy][:head] = req[:head]
  end

  def create_http_request(args)
    req = {
      :head => args[:head],
      :body => args[:body],
    }

    if (@proxy_opts)
      add_proxy_opts(req)
    end

    req
  end

  def make_logger()
    logger = Logger.new(STDOUT)
    logger.level = Logger::DEBUG
    logger
  end

  # Lets the cloud controller know we're alive and where it can find us
  def send_heartbeat
    @logger.info("Sending info to cloud controller: #{@offering_uri}")

    req = create_http_request(
      :head => @cc_req_hdrs,
      :body => @svc_json
    )

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

    req = create_http_request(
      :head => @cc_req_hdrs,
      :body => @deact_json
    )

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
    return if @fetching_handles

    @logger.info("Fetching handles from cloud controller @ #{@handles_uri}")
    @fetching_handles = true

    req = create_http_request :head => @cc_req_hdrs
    http = EM::HttpRequest.new(@handles_uri).get(req)

    http.callback do
      @fetching_handles = false
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
      @fetching_handles = false
      @logger.error("Failed fetching handles: #{http.error}")
    end
  end

  def http_uri(uri)
    uri = "http://#{uri}" unless (uri.index('http://') == 0 || uri.index('https://') == 0)
    uri
  end
end
