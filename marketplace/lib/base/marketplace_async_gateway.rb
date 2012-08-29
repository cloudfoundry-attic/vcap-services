# Copyright (c) 2009-2012 VMware, Inc.
require 'fiber'
require 'dm-types'

module VCAP
  module Services
    module Marketplace
      class MarketplaceAsyncServiceGateway < VCAP::Services::AsynchronousServiceGateway

        REQ_OPTS      = %w(mbus url token cloud_controller_uri).map {|o| o.to_sym}

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
          @node_timeout          = opts[:node_timeout]
          @logger                = opts[:logger] || make_logger()
          @token                 = opts[:token]
          @hb_interval           = opts[:heartbeat_interval] || 60
          @cld_ctrl_uri          = http_uri(opts[:cloud_controller_uri] || "api.vcap.me")
          @offering_uri          = "#{@cld_ctrl_uri}/services/v1/offerings/"
          @proxy_opts            = opts[:proxy]
          @handle_fetched        = true # set to true in order to compatible with base asycn gateway.

          @refresh_interval      = opts[:refresh_interval] || 300

          @marketplace_client = load_marketplace(opts)

          @catalog = {}

          token_hdrs = VCAP::Services::Api::GATEWAY_TOKEN_HEADER
          @cc_req_hdrs           = {
            'Content-Type' => 'application/json',
            token_hdrs     => @token,
          }

          Kernel.at_exit do
            if EM.reactor_running?
              on_exit(false)
            else
              EM.run { on_exit }
            end
          end

          @refresh_timer = EM::PeriodicTimer.new(@refresh_interval) do
            refresh_catalog_and_update_cc
          end

          ##### Start up
          @ready_to_serve = false

          refresh_catalog_and_update_cc
          @ready_to_serve = true
        end

        error [JsonMessage::ValidationError, JsonMessage::ParseError] do
          error_msg = ServiceError.new(ServiceError::MALFORMATTED_REQ).to_hash
          abort_request(error_msg)
        end

        not_found do
          error_msg = ServiceError.new(ServiceError::NOT_FOUND, request.path_info).to_hash
          abort_request(error_msg)
        end

        def refresh_catalog_and_update_cc
          f = Fiber.new do
            begin
              # get all service offerings
              refresh_catalog
              # active services in local database
              advertise_services
              # Ready to serve
              @logger.info("#{@marketplace_client.name} Marketplace Gateway is ready to serve incoming request.")
            rescue => e
              @logger.fatal("Error when refreshing #{@marketplace_client.name} catalog: #{fmt_error(e)}")
            end
          end
          f.resume
        end

        def refresh_catalog
          @catalog = @marketplace_client.get_catalog
        end

        def advertise_services(active=true)
          @catalog.each do |name, bsvc|
            req = @marketplace_client.generate_cc_advertise_request(name, bsvc, active)
            advertise_service_to_cc(req)
          end
        end

        def on_exit(stop_event_loop=true)
          @ready_to_serve = false
          @refresh_timer.cancel
          Fiber.new {
            # Since the services are not being stored locally
            refresh_catalog
            advertise_services(false)
            EM.stop if stop_event_loop
          }.resume
        end

        #################### Handlers ###################

        get "/" do
          return {"offerings" => @catalog, "ready_to_serve" => @ready_to_serve}.to_json
        end

        # Provision a marketplace service
        post "/gateway/v1/configurations" do
          @logger.info("Got request_body=#{request_body}")

          Fiber.new{
            msg = @marketplace_client.provision_service(request_body)
            if msg['success']
              resp = VCAP::Services::Api::GatewayHandleResponse.new(msg['response'])
              resp = resp.encode
              async_reply(resp)
            else
              async_reply_error(msg['response'])
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
            msg = @marketplace_client.bind_service_instance(params['service_id'], req)
            if msg['success']
              resp = VCAP::Services::Api::GatewayHandleResponse.new(msg['response'])
              resp = resp.encode
              async_reply(resp)
            else
              async_reply_error(msg['response'])
            end
          }.resume
          async_mode
        end

        # Unprovisions service instance
        delete "/gateway/v1/configurations/:service_id" do
          sid = params['service_id']
          @logger.debug("Unprovision request for service_id=#{sid}")
          Fiber.new {
            if @marketplace_client.unprovision_service(sid)
              async_reply
            else
              async_reply_error("Could not unprovision service #{sid}")
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
            if @marketplace_client.unbind_service(sid, hid)
              async_reply
            else
              async_reply_error("Could not unbind service #{sid} with handle id #{hid}")
            end

          }.resume
          async_mode
        end

        ################## Helpers ###################
        #
        helpers do

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

          def fmt_error(e)
            "#{e} [#{e.backtrace.join("|")}]"
          end
        end

      end
    end
  end
end
