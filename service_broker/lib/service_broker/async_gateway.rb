# Copyright (c) 2009-2011 VMware, Inc.
require 'fiber'
require 'dm-types'
require 'nats/client'
require 'uuidtools'

require 'base_async_gateway'
require 'service_error'
module VCAP
  module Services
    module ServiceBroker
    end
  end
end

class VCAP::Services::ServiceBroker::AsynchronousServiceGateway < VCAP::Services::BaseAsynchronousServiceGateway

  include VCAP::Services::Base::Error

  class BrokeredService
    include DataMapper::Resource
    # Custom table name
    storage_names[:default] = "brokered_services"

    property :label,       String,   :key => true
    property :name,        String,   :required => true
    property :version,     String,   :required => true
    property :credentials, Json,     :required => true
    property :acls,        Json

    # TODO: Make these required
    property :description, String,   :required => false
    property :provider,    String,   :required => false
  end

  REQ_OPTS      = %w(mbus external_uri token cloud_controller_uri).map {|o| o.to_sym}
  API_VERSION   = "v1"

  set :raise_errors, Proc.new {false}
  set :show_exceptions, false

  def initialize(opts)
    super(opts)
  end

  def setup(opts)
    missing_opts = REQ_OPTS.select {|o| !opts.has_key? o}
    raise ArgumentError, "Missing options: #{missing_opts.join(', ')}" unless missing_opts.empty?

    @host                  = opts[:host]
    @port                  = opts[:port]
    @node_timeout          = opts[:node_timeout]
    @logger                = opts[:logger] || make_logger()
    @token                 = opts[:token]
    @hb_interval           = opts[:heartbeat_interval] || 60
    @external_uri          = parse_uri(opts[:external_uri])
    @router_start_channel  = nil
    @proxy_opts            = opts[:proxy]
    @ready_to_serve        = false
    @handle_fetched        = true # set to true in order to compatible with base asycn gateway.
    @router_register_json  = {
      :host => @host,
      :port => @port,
      :uris => [ @external_uri.host ],
      :tags => {:components =>  "ServiceBroker"},
    }.to_json

    token_hdrs = VCAP::Services::Api::GATEWAY_TOKEN_HEADER
    @cc_req_hdrs           = {
      'Content-Type' => 'application/json',
      token_hdrs     => @token,
    }

    driver, path = opts[:local_db].split(':')
    db_dir = File.dirname(path)
    FileUtils.mkdir_p(db_dir)

    DataMapper.setup(:default, opts[:local_db])
    DataMapper::auto_upgrade!

    opts[:cloud_controller_uri] = http_uri(opts[:cloud_controller_uri])
    opts[:gateway_name]         = opts[:gateway_name] || "Service Broker"

    if opts[:cc_api_version] == "v1"
      require 'catalog_manager_v1'
      @catalog_manager = VCAP::Services::CatalogManagerV1.new(opts)
    elsif opts[:cc_api_version] == "v2"
      require 'catalog_manager_v2'
      @catalog_manager = VCAP::Services::CatalogManagerV2.new(opts)
    else
      raise "Unknown cc_api_version: #{opts[:cc_api_version]}"
    end

    Kernel.at_exit do
      if EM.reactor_running?
        on_exit(false)
      else
        EM.run { on_exit }
      end
    end

    migrate_saved_instances_on_startup

    ##### Start up
    f = Fiber.new do
      begin
        @logger.debug("Starting nats...")
        start_nats(opts[:mbus])

        # setup pre-defined offerings from config file
        @logger.debug("Setup predefined offerings")
        setup_pre_defined_services(opts[:service]) if opts[:service]

        # active services in local database
        @logger.debug("Advertise offerings to CC")
        advertise_saved_services

        # Ready to serve
        @logger.info("Service broker is ready to serve incoming request.")
        @ready_to_serve = true
      rescue => e
        @logger.fatal("Error when start up: #{fmt_error(e)}")
      end
    end
    f.resume
  end

  def migrate_saved_instances_on_startup
    BrokeredService.all.each do |bsvc|
      if bsvc.provider.to_s.empty?
        bsvc.provider = bsvc.name # default provider is name
        @logger.warn("Unable to set provider for: #{bsvc.inspect}") unless bsvc.save
      end
      if bsvc.description.to_s.empty?
        bsvc.description = bsvc.label # default description is label
        @logger.warn("Unable to set description for: #{bsvc.inspect}") unless bsvc.save
      end
   end
  end

  def parse_uri(uri_str)
    uri = URI.parse(uri_str)
    uri = URI.parse('http://' + uri_str) unless uri.scheme

    raise "Invalid external uri: #{uri_str}" unless uri.scheme.start_with? 'http'
    uri
  end

  def start_nats(uri)
    f = Fiber.current
    @nats = NATS.connect(:uri => uri) do
      on_connect_nats;
      f.resume
    end
    Fiber.yield
  end

  def on_connect_nats()
    @logger.info("Register service broker uri : #{@router_register_json}")
    @nats.publish('router.register', @router_register_json)
    @router_start_channel = @nats.subscribe('router.start') { @nats.publish('router.register', @router_register_json)}
  end

  def stop_nats()
    @nats.unsubscribe(@router_start_channel) if @router_start_channel
    @logger.debug("Unregister uri: #{@router_register_json}")
    @nats.publish("router.unregister", @router_register_json)
    @nats.close
  end

  def on_exit(stop_event_loop=true)
    @ready_to_serve = false
    Fiber.new {
      advertise_saved_services(false)
      stop_nats
      EM.stop if stop_event_loop
    }.resume
  end

  #################### Handlers ###################
  # Validate incoming request
  def validate_incoming_request
    unless @ready_to_serve
      error_msg = ServiceError.new(ServiceError::SERVICE_UNAVAILABLE).to_hash
      @logger.error("Not yet ready to serve: #{error_msg.inspect}")
      abort_request(error_msg)
    end

    unless request.media_type == Rack::Mime.mime_type('.json')
      error_msg = ServiceError.new(ServiceError::INVALID_CONTENT).to_hash
      @logger.error("Validation failure: #{error_msg.inspect}, request media type: #{request.media_type} is not json")
      abort_request(error_msg)
    end

    # Service auth token check
    unless auth_token && (auth_token == @token)
      error_msg = ServiceError.new(ServiceError::NOT_AUTHORIZED).to_hash
      @logger.error("Validation failure: #{error_msg.inspect}, expected token: #{@token}, specified token: #{auth_token}")
      abort_request(error_msg)
    end
  end

  error [JsonMessage::ValidationError, JsonMessage::ParseError] do
    error_msg = ServiceError.new(ServiceError::MALFORMATTED_REQ).to_hash
    @logger.error(error_msg.inspect)
    abort_request(error_msg)
  end

  not_found do
    error_msg = ServiceError.new(ServiceError::NOT_FOUND, request.path_info).to_hash
    @logger.error(error_msg.inspect)
    abort_request(error_msg)
  end

  ##### Service Brokers' brokered-service facing handlers ######

  # Advertise or modify a brokered service offerings
  post "/service-broker/#{API_VERSION}/offerings" do
    req = VCAP::Services::Api::ProxiedServiceOfferingRequest.decode(request_body)
    @logger.debug("Advertise brokered service for label=#{req.label}")

    Fiber.new {
      msg = advertise_brokered_service(req)
      if msg['success']
        async_reply
      else
        async_reply_error(msg['response'])
      end
    }.resume
    async_mode
  end

  # Delete a brokered service offerings
  delete "/service-broker/#{API_VERSION}/offerings/:label" do
    label = params[:label]
    @logger.debug("Delete brokered service for label=#{label}")

    Fiber.new {
      msg = delete_brokered_service(label)
      if msg['success']
        async_reply
      else
        async_reply_error(msg['response'])
      end
    }.resume
    async_mode
  end

  ##### Service Broker's CC facing handlers ######

  # Provision a brokered service
  post "/gateway/v1/configurations" do
    req =  VCAP::Services::Api::GatewayProvisionRequest.decode(request_body)
    @logger.info("Provision request for label=#{req.label} plan=#{req.plan}")

    Fiber.new {
      msg = provision_brokered_service(req)
      if msg['success']
        async_reply(VCAP::Services::Api::GatewayHandleResponse.new(msg['response']).encode)
      else
        async_reply_error(msg['response'])
      end
    }.resume
    async_mode
  end

  # Binding a brokered service
  post "/gateway/v1/configurations/:service_id/handles" do
    req = VCAP::Services::Api::GatewayBindRequest.decode(request_body)
    @logger.info("Binding request for service=#{params['service_id']} options=#{req.binding_options}")

    Fiber.new {
      msg = bind_brokered_service_instance(req.label, req.service_id, req.binding_options)
      if msg['success']
        async_reply(VCAP::Services::Api::GatewayHandleResponse.new(msg['response']).encode)
      else
        async_reply_error(msg['response'])
      end
    }.resume
    async_mode
  end

  # Unprovisions a brokered service instance
  delete "/gateway/v1/configurations/:service_id" do
    @logger.debug("Unprovision request for service_id=#{params['service_id']}")
    # simply return ok
    "{}"
  end

  # Unbinds a brokered service instance
  delete "/gateway/v1/configurations/:service_id/handles/:handle_id" do
    @logger.info("Unbind request for service_id=#{params['service_id']} handle_id=#{params['handle_id']}")
    # simply return ok
    "{}"
  end

  ############ Catalog management #############

  def advertise_saved_services(active=true)
     @catalog_manager.update_catalog(
       active,
       lambda { get_current_brokered_services_catalog },
       nil)
  end

  def get_current_brokered_services_catalog
    catalog = {}

    BrokeredService.all.each do |bsvc|
      key, entry = brokered_serivce_to_catalog_entry(bsvc)
      catalog[key] = entry
    end

    return catalog
  end

  def brokered_serivce_to_catalog_entry(bsvc)
    name, version = VCAP::Services::Api::Util.parse_label(bsvc.label)
    key = @catalog_manager.create_key(name, version, bsvc.provider)

    # TODO: DO NOT assume single 'default' plan with 'default plan' as description

    entry = {
      "id"                  => name,
      "version"             => version,
      "label"               => bsvc.label,
      "name"                => bsvc.name,
      "provider"            => bsvc.provider,
      "description"         => bsvc.description,
      "active"              => true,
      "acls"                => bsvc.acls,
      "url"                 => @external_uri.to_s,
      "plans"               => ["default"],
      "default_plan"        => "default",
      "tags"                => [],
      "timeout"             => @node_timeout,
      "supported_versions"  => [bsvc.version],
      "version_aliases"     => {:current => bsvc.version},
    }

    [key, entry]
  end

  def setup_pre_defined_services(services)
    services[:label] = "#{services[:name]}-#{services[:version]}"
    %w(name version).each {|key| services.delete(key.to_sym)}
    req = VCAP::Services::Api::ProxiedServiceOfferingRequest.new(services)
    advertise_brokered_service(req)
  rescue => e
    @logger.warn("Failed to advertise pre-defined services #{services.inspect}: #{e}")
  end

  ################## Helpers ###################
  helpers do

    def advertise_brokered_service(request)
      @logger.debug("Advertise a brokered service: #{request.inspect}")
      request.options.each do |opt|
        opt = VCAP.symbolize_keys(opt)

        name, version = VCAP::Services::Api::Util.parse_label(request.label)
        label = "#{name}_#{opt[:name]}-#{version}"
        provider = opt[:provider] || name # use name as provider unless explicitly specified

        # update or create local database entry
        bsvc = BrokeredService.get(label)
        if bsvc.nil?
          bsvc = BrokeredService.new
          bsvc.label       = label
          bsvc.version     = version
          bsvc.name        = name
          bsvc.provider    = provider
          bsvc.description = "#{request.description} (option '#{opt[:name]}')"
        end
        bsvc.credentials   = opt[:credentials]
        bsvc.acls          = opt[:acls]

        key, entry = brokered_serivce_to_catalog_entry(bsvc)
        svc = {}
        svc[key] = entry

        @catalog_manager.update_catalog(true, lambda { svc }, nil)

        if not bsvc.save
          @logger.error("Can't save entry to local database: #{bsvc.errors.inspect}")
        end
      end
      success()
    rescue => e
      if e.instance_of? ServiceError
        return failure(e)
      else
        @logger.warn("Can't advertise brokered service label=#{request.label}: #{fmt_error(e)}")
        return internal_fail
      end
    end

    def delete_brokered_service(label)
      name, version = VCAP::Services::Api::Util.parse_label(label)
      # Fetch all labels with given name
      bsvcs = BrokeredService.all(:name => name, :version => version)

      raise ServiceError.new(ServiceError::NOT_FOUND, "label #{label}") if bsvcs.empty?
      bsvcs.each do |bsvc|
        # TODO what if we got 404 error when delete a service?
        name, version = VCAP::Services::Api::Util.parse_label(bsvc.label)
        provider = bsvc.provider
        result = @catalog_manager.delete_offering(name, version, provider)

        next unless result
        @logger.error("Can't delete brokered service from local database: #{bsvc.errors.inspect}") unless bsvc.destroy
      end
      success()
    rescue => e
      if e.instance_of? ServiceError
        return failure(e)
      else
        @logger.warn("Can't delete brokered service label=#{label}: #{fmt_error(e)}")
        return internal_fail
      end
    end

    def provision_brokered_service(request)
      bsvc = BrokeredService.get(request.label)
      if bsvc
        svc = {
          :configuration => {:plan => request.plan},
          :credentials => bsvc.credentials,
          :service_id => UUIDTools::UUID.random_create.to_s,
        }
        @logger.debug("Brokered service provisioned #{svc.inspect}")
        success(svc)
      else
        @logger.warn("Can't find service label=#{request.label}")
        raise ServiceError.new(ServiceError::NOT_FOUND, request.label)
      end
    rescue => e
      if e.instance_of? ServiceError
        failure(e)
      else
        @logger.warn("Can't provision service label=#{request.label}: #{fmt_error(e)}")
        internal_fail
      end
    end

    def bind_brokered_service_instance(label, instance_id, binding_options, bind_handle=nil)
      bsvc = BrokeredService.get(label)
      if bsvc
        binding = {
          :configuration => {:data => {:binding_options => binding_options}},
          :credentials => bsvc.credentials,
          :service_id => UUIDTools::UUID.random_create.to_s,
        }
        @logger.debug("Generate new service binding: #{binding.inspect}")
        success(binding)
      else
        @logger.warn("Can't find service label=#{label}")
        raise ServiceError.new(ServiceError::NOT_FOUND, label)
      end
    rescue => e
      if e.instance_of? ServiceError
        failure(e)
      else
        @logger.warn("Can't bind service label=#{label}, id=#{instance_id}: #{fmt_error(e)}")
        internal_fail
      end
    end

    def fmt_error(e)
      "#{e} [#{e.backtrace.join("|")}]"
    end
  end

end
