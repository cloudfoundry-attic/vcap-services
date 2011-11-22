# Copyright (c) 2009-2011 VMware, Inc.
require 'fiber'
require 'dm-types'
require 'nats/client'
require 'uuidtools'

module VCAP
  module Services
    module ServiceBroker
    end
  end
end

class VCAP::Services::ServiceBroker::AsynchronousServiceGateway < VCAP::Services::AsynchronousServiceGateway

  class BrokeredService
    include DataMapper::Resource
    # Custom table name
    storage_names[:default] = "brokered_services"

    property :label,       String,   :key => true
    property :name,        String,   :required => true
    property :version,     String,   :required => true
    property :credentials, Json,     :required => true
    property :acls,        Json,     :required => true
  end

  REQ_OPTS      = %w(mbus external_uri token cloud_controller_uri).map {|o| o.to_sym}
  API_VERSION   = "poc"

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
    @cld_ctrl_uri          = http_uri(opts[:cloud_controller_uri])
    @external_uri          = opts[:external_uri]
    @offering_uri          = "#{@cld_ctrl_uri}/services/v1/offerings/"
    @service_list_uri      = "#{@cld_ctrl_uri}/brokered_services/poc/offerings"
    @router_start_channel  = nil
    @proxy_opts            = opts[:proxy]
    @ready_to_serve        = false
    @handle_fetched        = true # set to true in order to compatible with base asycn gateway.
    @router_register_json  = {
      :host => @host,
      :port => @port,
      :uris => [ @external_uri ],
      :tags => {:components =>  "ServiceBroker"},
    }.to_json

    token_hdrs = VCAP::Services::Api::GATEWAY_TOKEN_HEADER
    @cc_req_hdrs           = {
      'Content-Type' => 'application/json',
      token_hdrs     => @token,
    }

    DataMapper.setup(:default, opts[:local_db])
    DataMapper::auto_upgrade!

    Kernel.at_exit do
      if EM.reactor_running?
        on_exit(false)
      else
        EM.run { on_exit }
      end
    end

    ##### Start up
    f = Fiber.new do
      begin
        start_nats(opts[:mbus])
        # get all brokered service offerings
        fetch_brokered_services
        # active services in local database
        advertise_saved_services
        # active predefined offerings
        advertise_pre_defined_services(opts[:service]) if opts[:service]
        # Ready to serve
        @logger.info("Service broker is ready to serve incoming request.")
        @ready_to_serve = true
      rescue => e
        @logger.fatal("Error when start up: #{fmt_error(e)}")
      end
    end
    f.resume
  end

  # Validate the incoming request
  before do
    unless @ready_to_serve
      error_msg = ServiceError.new(ServiceError::SERVICE_UNAVAILABLE).to_hash
      abort_request(error_msg)
    end
  end

  error [JsonMessage::ValidationError, JsonMessage::ParseError] do
    error_msg = ServiceError.new(ServiceError::MALFORMATTED_REQ).to_hash
    abort_request(error_msg)
  end

  not_found do
    error_msg = ServiceError.new(ServiceError::NOT_FOUND, request.path_info).to_hash
    abort_request(error_msg)
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

  def fetch_brokered_services
    f = Fiber.current
    req = create_http_request(
      :head => @cc_req_hdrs
    )

    f = Fiber.current
    http = EM::HttpRequest.new(@service_list_uri).get(req)
    http.callback { f.resume(http) }
    http.errback { f.resume(http) }
    Fiber.yield

    if http.error.empty?
      if http.response_header.status == 200
        # For V1, we can't get enough information such as services credentials from CC.
        # If CC return a service label that not known by SB, we simply print it out rather than serve it.
        resp = VCAP::Services::Api::ListBrokeredServicesResponse.decode(http.response)
        resp.brokered_services.each {|bsvc| @logger.info("Fetch brokered service from CC: label=#{bsvc["label"]}")}
        return true
      else
        @logger.warn("Failed to fetch brokered services, status=#{http.response_header.status}")
      end
    else
      @logger.warn("Failed to fetch brokered services: #{http.error}")
    end
    nil
  rescue => e
    @logger.warn("Failed to fetch brokered services: #{fmt_error(e)}")
  end

  def advertise_saved_services(active=true)
    BrokeredService.all.each do |bsvc|
      req = {}
      req[:label] = bsvc.label
      req[:active] = active
      req[:acls] = bsvc.acls
      req[:url] = "http://#{@external_uri}"
      req[:plans] = ["default"]
      req[:tags] = ["default"]
      advertise_brokered_service_to_cc(req)
    end
  end

  def advertise_pre_defined_services(services)
    services[:label] = "#{services[:name]}-#{services[:version]}"
    %w(name version).each {|key| services.delete(key.to_sym)}
    req = VCAP::Services::Api::BrokeredServiceOfferingRequest.new(services)
    advertise_brokered_service(req)
  rescue => e
    @logger.warn("Failed to advertise pre-defined services #{services.inspect}: #{e}")
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

  # Advertise or modify a brokered service offerings
  post "/service-broker/#{API_VERSION}/offerings" do
    req = VCAP::Services::Api::BrokeredServiceOfferingRequest.decode(request_body)
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

  # Provision a brokered service
  post "/gateway/v1/configurations" do
    req =  VCAP::Services::Api::GatewayProvisionRequest.decode(request_body)
    @logger.info("Provision request for label=#{req.label} plan=#{req.plan}")

    Fiber.new {
      msg = provision_brokered_service(req)
      if msg['success']
        async_reply(VCAP::Services::Api::GatewayProvisionResponse.new(msg['response']).encode)
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
        async_reply(VCAP::Services::Api::GatewayBindResponse.new(msg['response']).encode)
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

  ################## Helpers ###################
  #
  helpers do

    def advertise_brokered_service(request)
      @logger.debug("Advertise a brokerd service: #{request.inspect}")
      label = request.label
      des = request.description
      options = request.options

      options.each do |opt|
        opt = VCAP.symbolize_keys(opt)
        svc = {}
        name, version = VCAP::Services::Api::Util.parse_label(label)
        svc[:label] = "#{name}_#{opt[:name]}-#{version}"
        svc[:active] = true
        svc[:description] = "#{des} (option '#{opt[:name]}')"
        # Add required fields
        svc[:acls] = opt[:acls]
        svc[:url] = "http://#{@external_uri}"
        svc[:plans] = ["default"]
        svc[:tags] = ["default"]

        # update or create local database entry
        bsvc = BrokeredService.get(svc[:label])
        if bsvc.nil?
          bsvc = BrokeredService.new
          bsvc.label = svc[:label]
          bsvc.version = version
          bsvc.name = name
        end
        bsvc.credentials = opt[:credentials]
        bsvc.acls = opt[:acls]
        result = advertise_brokered_service_to_cc(svc)
        if result
          if not bsvc.save
            @logger.error("Can't save entry to local database: #{bsvc.errors.inspect}")
          end
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
      @logger.debug("Delete brokerd service: label=#{label}")
      name, version = VCAP::Services::Api::Util.parse_label(label)
      # Fetch all labels with given name
      bsvcs = BrokeredService.all(:name => name, :version => version)

      raise ServiceError.new(ServiceError::NOT_FOUND, "label #{label}") if bsvcs.empty?
      bsvcs.each do |bsvc|
        # TODO what if we got 404 error when delete a service?
        result = delete_offerings(bsvc.label)
        if not result
          next
        end
        if not bsvc.destroy
          @logger.error("Can't delete brokered service from local database: #{bsvc.errors.inspect}")
        end
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

    def advertise_brokered_service_to_cc(offering)
      @logger.debug("advertise service offering to cloud_controller:#{offering.inspect}")
      return false unless offering

      req = create_http_request(
        :head => @cc_req_hdrs,
        :body => Yajl::Encoder.encode(offering),
      )

      f = Fiber.current
      http = EM::HttpRequest.new(@offering_uri).post(req)
      http.callback { f.resume(http) }
      http.errback { f.resume(http) }
      Fiber.yield

      if http.error.empty?
        if http.response_header.status == 200
          @logger.info("Successfully advertise offerings #{offering.inspect}")
          return true
        else
          @logger.warn("Failed advertise offerings:#{offering.inspect}, status=#{http.response_header.status}")
        end
      else
        @logger.warn("Failed advertise offerings:#{offering.inspect}: #{http.error}")
      end
      return false
    end

    def delete_offerings(label)
      return false unless label

      req = create_http_request(:head => @cc_req_hdrs)
      uri = URI.join(@offering_uri, label)
      f = Fiber.current
      http = EM::HttpRequest.new(uri).delete(req)
      http.callback { f.resume(http) }
      http.errback { f.resume(http) }
      Fiber.yield

      if http.error.empty?
        if http.response_header.status == 200
          @logger.info("Successfully delete offerings label=#{label}")
          return true
        else
          @logger.warn("Failed delete offerings label=#{label}, status=#{http.response_header.status}")
        end
      else
        @logger.warn("Failed delete offerings label=#{label}: #{http.error}")
      end
      return false
    end

    def provision_brokered_service(request)
      bsvc = BrokeredService.get(request.label)
      if bsvc
        svc = {
          :data => {:plan => request.plan},
          :credentials => bsvc.credentials,
          :service_id => UUIDTools::UUID.random_create.to_s,
        }
        @logger.debug("Brokered service provisioned #{svc.inspect}")
        success(svc)
      else
        @logger.warn("Can't find service label=#{request.label}")
        raise ServiceError.new(ServiceError::NOT_FOUND, req.label)
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
