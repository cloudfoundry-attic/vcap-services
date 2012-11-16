# Copyright (c) 2009-2012 VMware, Inc.
require 'fiber'
require 'nats/client'
require 'uri'
require 'timeout'

require 'vcap/component'
require_relative 'cc_client_v2'

module VCAP
  module Services
    module Marketplace
      class MarketplaceAsyncServiceGatewayV2 < VCAP::Services::AsynchronousServiceGateway

        REQ_OPTS    = %w(mbus external_uri cloud_controller_uri).map {|o| o.to_sym}

        set :raise_errors, Proc.new {false}
        set :show_exceptions, false

        def initialize(opts)
          super(opts)
        end

        def load_marketplace(opts)
          marketplace_lib_path = File.join(File.dirname(__FILE__), '..', 'marketplaces', opts[:marketplace])
          @logger.info("Loading marketplace: #{opts[:marketplace]} from: #{marketplace_lib_path}")

          $LOAD_PATH.unshift(marketplace_lib_path)
          Dir[marketplace_lib_path + '/*.rb'].each do |file|
            f = File.basename(file, File.extname(file))
            require f
          end

          # To minimize the amount of marketplace-specific code, the config file specifies the class that
          # implements MarketplaceBase's abstract methods for this marketplace. So we need to translate the
          # name of the class into the actual class object, and then create an instance of it.
          klass = eval(opts[:classname])
          klass.new(opts)
        end

        def setup(opts)
          missing_opts = REQ_OPTS.select {|o| !opts.has_key? o}
          raise ArgumentError, "Missing options: #{missing_opts.join(', ')}" unless missing_opts.empty?

          @host                  = opts[:host]
          @port                  = opts[:port]
          @external_uri          = opts[:external_uri]
          @router_register_uri   = (URI.parse(@external_uri)).host
          @node_timeout          = opts[:node_timeout]
          @logger                = opts[:logger] || make_logger()
          @token                 = opts[:token]
          @index                 = opts[:index] || 0
          @hb_interval           = opts[:heartbeat_interval] || 60

          @proxy_opts            = opts[:proxy]
          @handle_fetched        = true # set to true in order to compatible with base asycn gateway.

          @refresh_interval      = opts[:refresh_interval] || 300

          @marketplace_client    = load_marketplace(opts)

          @component_host        = opts[:host]
          @component_port        = opts[:component_port] || VCAP.grab_ephemeral_port
          @component_user        = opts[:user] || VCAP.secure_uuid
          @component_pass        = opts[:password] || VCAP.secure_uuid

          @router_register_json = {
            :host => @host,
            :port => @port,
            :uris => [ @router_register_uri ],
            :tags => {:components => "#{@marketplace_client.name}MarketplaceGateway" }
          }.to_json

          @catalog = {}
          @marketplace_gateway_varz_details = {}

          opts[:cloud_controller_uri]    = http_uri(opts[:cloud_controller_uri] || "api.vcap.me")
          opts[:marketplace_client_name] = @marketplace_client.name
          @cc_client = VCAP::Services::Marketplace::CCNGClient.new(opts)

          Kernel.at_exit do
            if EM.reactor_running?
              on_exit(false)
            else
              EM.run { on_exit }
            end
          end

          f = Fiber.new do
            start_nats(opts[:mbus])
          end
          f.resume

          @refresh_timer = EM::PeriodicTimer.new(@refresh_interval) do
            refresh_catalog_and_update_cc
          end

          z_interval = opts[:z_interval] || 30
          EM.add_periodic_timer(z_interval) do
            EM.defer { update_varz }
          end

          @stats_lock = Mutex.new
          @stats = {}
          snapshot_and_reset_stats

          refresh_catalog_and_update_cc
        end

        error [JsonMessage::ValidationError, JsonMessage::ParseError] do
          error_msg = ServiceError.new(ServiceError::MALFORMATTED_REQ).to_hash
          abort_request(error_msg)
        end

        not_found do
          error_msg = ServiceError.new(ServiceError::NOT_FOUND, request.path_info).to_hash
          abort_request(error_msg)
        end

        ############ Catalog processing ##########

        def refresh_catalog_and_update_cc
          @logger.info("Refreshing Catalog...")
          f = Fiber.new do
            begin
              refresh_catalog

              advertise_services

              # Ready to serve
              update_varz
              @logger.info("#{@marketplace_client.name} Marketplace Gateway is ready to serve incoming request.")
            rescue => e
              @logger.warn("Error when processing #{@marketplace_client.name} catalog: #{fmt_error(e)}")
            end
         end
          f.resume
        end

        def refresh_catalog
          failed = false
          begin
            @catalog_in_ccdb = @cc_client.get_registered_services_from_cc
          rescue => e
            failed = true
            @logger.error("Failed to get proxied services from cc: #{e.inspect}")
          ensure
            update_stats("refresh_cc_services", failed)
          end

          failed = false
          begin
            Timeout::timeout(@node_timeout) do
              @catalog_in_marketplace = @marketplace_client.get_catalog
            end
          rescue => e1
            failed = true
            @logger.error("Failed to get catalog from marketplace: #{e1.inspect}")
          ensure
            update_stats("refresh_catalog", failed)
          end
        end

        def process_plans(plans_from_marketplace, plans_already_in_cc)
          plans_to_add = []
          plans_to_update = {}

          marketplace_plans = plans_from_marketplace.keys
          registered_plans  = plans_already_in_cc.keys

          # Update active plans
          # active plans = intersection of (marketplace_plans & registered_plans)
          active_plans = marketplace_plans & registered_plans
          active_plans.each { |plan_name|
            plan_details = plans_from_marketplace[plan_name]

            # TODO: is this really necessary? keeping this for now
            # Currently the only changeable aspect is the descritption
            if plan_details["description"] != plans_already_in_cc[plan_name]["description"]
              plan_guid = plans_already_in_cc[plan_name]["guid"]
              plans_to_update[plan_guid] = { "name" => plan_name, "description" => plan_details["description"] }
              @logger.debug("Updating plan: #{plan_name} to: #{plans_to_update[plan_guid].inspect}")
            else
              @logger.debug("No changes to plan: #{plan_name}")
            end
          }

          # Add new plans -> marketplace_plans - active_plans
          new_plans = marketplace_plans - active_plans
          new_plans.each { |plan_name|
            @logger.debug("Adding new plan: #{plans_from_marketplace[plan_name].inspect}")
            plans_to_add << plans_from_marketplace[plan_name]
          }

          # TODO: What to do with deactivated plans?
          # Should handle this manually for now?
          deactivated_plans = registered_plans - active_plans
          @logger.warn("Found #{deactivated_plans.size} deactivated plans: - #{deactivated_plans.inspect}")

          [ plans_to_add, plans_to_update ]
        end

        def advertise_services(active=true)
          @logger.info("#{active ? "Activate" : "Deactivate"} services...")
          if !(@catalog_in_marketplace && @catalog_in_ccdb)
            @logger.warn("Cannot  advertise services since the catalog from either marketplace or ccdb could not be retrieved")
            return
          end

          # Set services missing from marketplace offerings to inactive
          # Process all services currently in marketplace
          # NOTE: Existing service offerings in ccdb will have a guid and require a PUT operation for update
          #       New service offerings will not have guid and require POST operation for create

          registered_offerings  = @catalog_in_ccdb.keys
          marketplace_offerings = @catalog_in_marketplace.keys
          @logger.debug("registered: #{registered_offerings.inspect}, marketplace: #{marketplace_offerings.inspect}")

          # POST updates to active and disabled services
          # Active offerings is intersection of marketplace and ccdb offerings, we only need to update these
          active_offerings = marketplace_offerings & registered_offerings
          active_offerings.each do |label|
            svc = @catalog_in_marketplace[label]
            req, plans = @marketplace_client.generate_ccng_advertise_request(svc, active)
            guid = (@catalog_in_ccdb[label])["guid"]

            plans_to_add, plans_to_update = process_plans(plans, @catalog_in_ccdb[label]["plans"])

            @logger.debug("Refresh offering: #{req.inspect}")
            advertise_service_to_cc(req, guid, plans_to_add, plans_to_update)
          end

          # Inactive offerings is ccdb_offerings - active_offerings
          inactive_offerings = registered_offerings - active_offerings
          inactive_offerings.each do |label|
            svc     = @catalog_in_ccdb[label]
            guid    = svc["guid"]
            service = svc["service"]
            service[:active] = false

            req, plans = @marketplace_client.generate_ccng_advertise_request(service, false)

            @logger.debug("Deactivating offering: #{req.inspect}")
            advertise_service_to_cc(req, guid, [], {}) # don't touch plans, just deactivate
          end

          # PUT new offerings (yet to be registered) = marketplace_offerings - active_offerings
          new_offerings = marketplace_offerings - active_offerings
          new_offerings.each do |label|
            svc = @catalog_in_marketplace[label]
            req, plans_to_add = @marketplace_client.generate_ccng_advertise_request(svc, active)

            @logger.debug("Add new offering: #{req.inspect}")
            advertise_service_to_cc(req, nil, plans_to_add, {}) # nil guid => new service, so add all plans
          end

          active_count = active ? active_offerings.size : 0
          disabled_count = inactive_offerings.size + (active ? 0 : active_offerings.size)

          @logger.info("Found #{active_count} active, #{disabled_count} disabled and #{new_offerings.size} new service offerings")

          @marketplace_gateway_varz_details[:active_offerings] = active_count
          @marketplace_gateway_varz_details[:disabled_services] = disabled_count
        end

        def advertise_service_to_cc(req, service_guid, plans_to_add, plans_to_update)
          result = @cc_client.advertise_service_to_cc(req, service_guid, plans_to_add, plans_to_update)
          update_stats("advertise_services", !result)
          result
        end



        ############  Varz Processing ##############

        def snapshot_and_reset_stats
          stats_snapshot = {}
          @stats_lock.synchronize do
            stats_snapshot = @stats.dup
            @stats[:provision_requests]   = 0
            @stats[:provision_failures]   = 0
            @stats[:unprovision_requests] = 0
            @stats[:unprovision_failures] = 0
            @stats[:bind_requests]        = 0
            @stats[:bind_failures]        = 0
            @stats[:unbind_requests]      = 0
            @stats[:unbind_failures]      = 0

            @stats[:refresh_catalog_requests]     = 0
            @stats[:refresh_catalog_failures]     = 0
            @stats[:refresh_cc_services_requests] = 0
            @stats[:refresh_cc_services_failures] = 0
            @stats[:advertise_services_requests]  = 0
            @stats[:advertise_services_failures]  = 0
          end
          stats_snapshot
        end

        def update_stats(op_name, failed)
          op_key = "#{op_name}_requests".to_sym
          op_failure_key = "#{op_name}_failures".to_sym

          @stats_lock.synchronize do
            @stats[op_key] += 1
            @stats[op_failure_key] += 1 if failed
          end
        end

        def update_varz
          VCAP::Component.varz["marketplace_gateway"] = @marketplace_gateway_varz_details
          VCAP::Component.varz["stats"] = snapshot_and_reset_stats
          VCAP::Component.varz[@marketplace_client.name.downcase] = @marketplace_client.varz_details if @marketplace_client.varz_details.size > 0
        end

        ################ Nats Handlers (setup / cleanup) #############

        def start_nats(uri)
          f = Fiber.current
          @nats = NATS.connect(:uri => uri) do
            VCAP::Component.register(
              :nats  => @nats,
              :type  => "#{@marketplace_client.name}MarketplaceGateway",
              :host  => @component_host,
              :port  => @component_port,
              :index => @index,
              :user  => @component_user,
              :password => @component_pass
            )
            on_connect_nats;
            f.resume
          end
          Fiber.yield
       end

        def on_connect_nats()
          @logger.info("Register #{@marketplace_client.name} marketplace gateway: #{@router_register_json}")
          @nats.publish('router.register', @router_register_json)
          @router_start_channel = @nats.subscribe('router.start') {
            @nats.publish('router.register', @router_register_json)
          }
        end

        def stop_nats()
          @nats.unsubscribe(@router_start_channel) if @router_start_channel
          @logger.debug("Unregister #{@marketplace_client.name} marketplace gateway: #{@router_register_json}")
          @nats.publish("router.unregister", @router_register_json)
          sleep 0.1 # Allow some time for actual de-registering before shutting down
          @nats.close
        end

        def on_exit(stop_event_loop=true)
          @refresh_timer.cancel
          Fiber.new {
            # Since the services are not being stored locally
            refresh_catalog
            advertise_services(false)
            stop_nats
            EM.stop if stop_event_loop
          }.resume
        end

        ################## Helpers ###################
        #
        helpers do

          def reply_error(resp='{}')
            async_reply_raw(500, {'Content-Type' => Rack::Mime.mime_type('.json')}, resp)
          end

          def fmt_error(e)
            "#{e} [#{e.backtrace.join("|")}]"
          end
        end

        #################### Handlers ###################

        # Helpers for unit testing
        post "/marketplace/set/:key/:value" do
          @logger.info("TEST HELPER ENDPOINT - set: key=#{params[:key]}, value=#{params[:value]}")
          Fiber.new {
            begin
              @marketplace_client.set_config(params[:key], params[:value])
              refresh_catalog_and_update_cc
              async_reply("")
            rescue => e
              reply_error(e.inspect)
            end
          }.resume
          async_mode
        end

        get "/" do
          return {"marketplace" => @marketplace_client.name, "offerings" => @catalog_in_marketplace}.to_json
        end

        # Provision a marketplace service
        post "/gateway/v1/configurations" do
          @logger.info("Got request_body=#{request_body}")

          Fiber.new{
            failed = false
            begin
              msg = Timeout::timeout(@node_timeout) do
                @marketplace_client.provision_service(request_body)
              end
              resp = VCAP::Services::Api::GatewayHandleResponse.new(msg).encode
              async_reply(resp)
            rescue => e
              failed = true
              reply_error(e.inspect)
            ensure
              update_stats("provision", failed)
            end
          }.resume
          async_mode
        end

        # Binding a service
        post "/gateway/v1/configurations/:service_id/handles" do
          @logger.info("Got request_body=#{request_body}")
          req = VCAP::Services::Api::GatewayBindRequest.decode(request_body)
          @logger.info("Binding request for service=#{params['service_id']} options=#{req.inspect}")

          Fiber.new {
            failed = false
            begin
              msg = Timeout::timeout(@node_timeout) do
                @marketplace_client.bind_service_instance(params['service_id'], req)
              end
              resp = VCAP::Services::Api::GatewayHandleResponse.new(msg).encode
              async_reply(resp)
            rescue => e
              failed = true
              reply_error(e.inspect)
            ensure
              update_stats("bind", failed)
            end
          }.resume
          async_mode
        end

        # Unprovisions service instance
        delete "/gateway/v1/configurations/:service_id" do
          sid = params['service_id']
          @logger.debug("Unprovision request for service_id=#{sid}")
          Fiber.new {
            failed = false
            begin
              Timeout::timeout(@node_timeout) do
                @marketplace_client.unprovision_service(sid)
              end
              async_reply
            rescue => e
              failed = true
              reply_error(e.inspect)
            ensure
              update_stats("unprovision", failed)
            end
          }.resume
          async_mode
        end

        # Unbinds a provisioned service instance
        delete "/gateway/v1/configurations/:service_id/handles/:handle_id" do
          sid = params['service_id']
          hid = params['handle_id']
          @logger.info("Unbind request for service_id=#{sid} handle_id=#{hid}")
          Fiber.new {
            failed = false
            begin
              Timeout::timeout(@node_timeout) do
                @marketplace_client.unbind_service(sid, hid)
              end
              async_reply
            rescue => e
              failed = true
              reply_error(e.inspect)
            ensure
              update_stats("unbind", failed)
            end

          }.resume
          async_mode
        end

      end
    end
  end
end
