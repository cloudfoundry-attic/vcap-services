# Copyright (c) 2009-2012 VMware, Inc.
require 'fiber'
require 'nats/client'
require 'uri'
require 'timeout'

require 'vcap/component'

require 'base_async_gateway'
require 'catalog_manager_base'

module VCAP
  module Services
    module Marketplace
      class MarketplaceServiceGateway < VCAP::Services::BaseAsynchronousServiceGateway

        REQ_OPTS = %w(cc_api_version mbus external_uri cloud_controller_uri service_auth_tokens).map {|o| o.to_sym}

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

          @refresh_interval      = opts[:refresh_interval] || 300
          @service_auth_tokens   = opts[:service_auth_tokens]

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
          @stats = {}
          @stats_lock = Mutex.new

          opts[:cloud_controller_uri]    = http_uri(opts[:cloud_controller_uri] || "api.vcap.me")
          opts[:gateway_name] = @marketplace_client.name

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

          f = Fiber.new do
            start_nats(opts[:mbus])
          end
          f.resume

          z_interval = opts[:z_interval] || 30
          EM.add_periodic_timer(z_interval) do
            EM.defer { update_varz }
          end

          @refresh_timer = EM::PeriodicTimer.new(@refresh_interval) do
            refresh_catalog_and_update_cc(true)
          end

          EM.next_tick {
            snapshot_and_reset_stats
            refresh_catalog_and_update_cc(true)
          }
        end

        ############ Catalog processing ##########

        def refresh_catalog_and_update_cc(activate)
          @logger.info("Refreshing Catalog...")
          @catalog_manager.update_catalog(
            activate,
            lambda {
              Timeout::timeout(@node_timeout) do
                return @marketplace_client.get_catalog
              end
            },
            lambda {
              update_varz
            }
          )
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

            catalog_manager_stats = @catalog_manager.snapshot_and_reset_stats
            stats_snapshot = stats_snapshot.merge(catalog_manager_stats)
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
          VCAP::Component.varz["stats"] = snapshot_and_reset_stats
          VCAP::Component.varz[@marketplace_client.name.downcase] = @marketplace_client.varz_details if @marketplace_client.varz_details.size > 0
        end

        ################ Nats Handlers (setup / cleanup) #############

        def start_nats(uri)
          f = Fiber.current
          @nats = NATS.connect(:uri => uri) do
            VCAP::Component.register(
              :nats     => @nats,
              :type     => "#{@marketplace_client.name}MarketplaceGateway",
              :host     => @component_host,
              :port     => @component_port,
              :index    => @index,
              :user     => @component_user,
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
            refresh_catalog_and_update_cc(false)
            stop_nats
            EM.stop if stop_event_loop
         }.resume
        end

        ################## Helpers ###################
        #
        helpers do

          def is_a_valid_auth_token(token)
            valid = @service_auth_tokens.values.include?(token)
            @logger.error("Requested token: #{token} is invalid") if !valid
            valid
          end

          def reply_error(resp='{}')
            async_reply_raw(500, {'Content-Type' => Rack::Mime.mime_type('.json')}, resp)
          end

          def fmt_error(e)
            "#{e} [#{e.backtrace.join("|")}]"
          end

        end

        #################### Handlers ###################

        # Validate incoming request
        def validate_incoming_request
          unless request.media_type == Rack::Mime.mime_type('.json')
            error_msg = ServiceError.new(ServiceError::INVALID_CONTENT).to_hash
            @logger.error("Validation failure: #{error_msg.inspect}")
            abort_request(error_msg)
          end

          unless auth_token && is_a_valid_auth_token(auth_token)
            error_msg = ServiceError.new(ServiceError::NOT_AUTHORIZED).to_hash
            @logger.error("Validation failure: #{error_msg.inspect}")
            abort_request(error_msg)
          end

          content_type :json
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
                @marketplace_client.bind_service_instance(params['service_id'], req.binding_options)
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
