# Copyright (c) 2009-2012 VMware, Inc.
require 'fiber'
require 'nats/client'
require 'uri'
require 'timeout'

require 'vcap/component'

module VCAP
  module Services
    module Marketplace
      class MarketplaceAsyncServiceGateway < VCAP::Services::AsynchronousServiceGateway

        API_VERSION = "v1"
        REQ_OPTS    = %w(mbus external_uri token cloud_controller_uri).map {|o| o.to_sym}

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
          @cld_ctrl_uri          = http_uri(opts[:cloud_controller_uri] || "api.vcap.me")
          @offering_uri          = "#{@cld_ctrl_uri}/services/#{API_VERSION}/offerings"
          @service_list_uri      = "#{@cld_ctrl_uri}/proxied_services/#{API_VERSION}/offerings"
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

          token_hdrs = VCAP::Services::Api::GATEWAY_TOKEN_HEADER
          @cc_req_hdrs           = {
            'Content-Type' => 'application/json',
            token_hdrs     => @token,
          }

          @marketplace_gateway_varz_details = {}

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
              @logger.warn("Error when refreshing #{@marketplace_client.name} catalog: #{fmt_error(e)}")
            end
         end
          f.resume
        end

        def refresh_catalog
          failed = false
          begin
            @catalog_in_ccdb = get_proxied_services_from_cc
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

        def advertise_services(active=true)
          # Set services missing from marketplace offerings to inactive
          deactivate_disabled_services

          # Process all services currently in marketplace
          @catalog_in_marketplace.each do |label, bsvc|
            req = @marketplace_client.generate_cc_advertise_request(bsvc["id"], bsvc, active)
            advertise_service_to_cc(req)
          end

          @marketplace_gateway_varz_details[:active_offerings] = @catalog_in_marketplace.size
        end

        def deactivate_disabled_services
          disabled_count = 0

          current_offerings = []
          @catalog_in_marketplace.each { |k, v|
            current_offerings << v["id"]
          }

          @catalog_in_ccdb.each do |label, svc|
            service_name, version = label.split(/-/)

            if (@marketplace_client.offering_disabled?(service_name, current_offerings))
              req = svc.dup
              req[:active] = false

              @logger.warn("#{@marketplace_client.name} service offering: #{label} not found in latest offering. Deactivating...")
              advertise_service_to_cc(req)

              disabled_count += 1
            else
              @logger.debug("Offering #{label} still active in #{@marketplace_client.name} marketplace")
            end
          end

          @marketplace_gateway_varz_details[:disabled_services] = disabled_count
          @logger.info("Found #{disabled_count} disabled service offerings")
        end

        def update_varz
          VCAP::Component.varz["marketplace_gateway"] = @marketplace_gateway_varz_details
          VCAP::Component.varz["stats"] = snapshot_and_reset_stats
          VCAP::Component.varz[@marketplace_client.name.downcase] = @marketplace_client.varz_details if @marketplace_client.varz_details.size > 0
        end

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

        ################## Helpers ###################
        #
        helpers do

          def reply_error(resp='{}')
            async_reply_raw(500, {'Content-Type' => Rack::Mime.mime_type('.json')}, resp)
          end

          def get_proxied_services_from_cc
            @logger.debug("Get proxied services from cloud_controller: #{@service_list_uri}")
            services = {}
            req = create_http_request( :head => @cc_req_hdrs )

            f = Fiber.current
            http = EM::HttpRequest.new(@service_list_uri).get(req)
            http.callback { f.resume(http) }
            http.errback { f.resume(http) }
            Fiber.yield

            if http.error.empty?
              if http.response_header.status == 200
                resp = VCAP::Services::Api::ListProxiedServicesResponse.decode(http.response)
                resp.proxied_services.each {|bsvc|
                  @logger.info("Fetch #{@marketplace_client.name} service from CC: label=#{bsvc["label"]} - #{bsvc.inspect}")
                  services[bsvc["label"]] = bsvc
                }
              else
                @logger.warn("Failed to fetch #{@marketplace_client.name} service from CC - status=#{http.response_header.status}")
              end
            else
              @logger.warn("Failed to fetch #{@marketplace_client.name} service from CC: #{http.error}")
            end

            return services
          end

          def advertise_service_to_cc(offering)
            @logger.debug("advertise service offering #{offering.inspect} to cloud_controller: #{@offering_uri}")
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
                update_stats("advertise_services", false)
                return true
              else
                @logger.warn("Failed advertise offerings:#{offering.inspect}, status=#{http.response_header.status}")
              end
            else
              @logger.warn("Failed advertise offerings:#{offering.inspect}: #{http.error}")
            end

            update_stats("advertise_services", true)
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

          def fmt_error(e)
            "#{e} [#{e.backtrace.join("|")}]"
          end
        end

      end
    end
  end
end
