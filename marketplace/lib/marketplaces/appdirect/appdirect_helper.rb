# Copyright (c) 2009-2012 VMware, Inc.
require "oauth"
require "json"
require "fiber"

require_relative "appdirect_error"
require_relative "app_direct_catalog"
require_relative "offering_whitelist"

module VCAP
  module Services
    module Marketplace
      module Appdirect

        class AppdirectHelper
          include VCAP::Services::Base::Error

          SERVICES_PATH = "custom/cloudfoundry/v1/services"

          HEADER = {"Content-Type" => "application/json" , "Accept"=>"application/json"}
          REQ_CONFIG = %w(endpoint key secret).map {|o| o.to_sym}

          attr_reader :offering_whitelist, :logger, :app_direct_catalog

          def initialize(opts, logger)
            @logger = logger

            raise("No appdirect config section provided in: #{opts.inspect}") unless opts[:appdirect]

            appdirect_config = opts[:appdirect]

            missing_opts = REQ_CONFIG.select {|o| !appdirect_config.has_key? o}
            raise ArgumentError, "Missing options: #{missing_opts.join(', ')}" unless missing_opts.empty?

            @appdirect_endpoint = appdirect_config[:endpoint]
            @offering_whitelist = OfferingWhitelist.new(opts[:offering_whitelist], logger)
            @app_direct_catalog = AppDirectCatalog.new(@appdirect_endpoint,
                                 method(:perform_request),
                                 logger)
            @test_mode          = opts[:test_mode]

            if !@test_mode
              consumer = OAuth::Consumer.new(appdirect_config[:key], appdirect_config[:secret])
              @access_token = OAuth::AccessToken.new(consumer)
            end
          end

          # helper method
          def perform_request(verb, url, header, body)
            if @test_mode
              f = Fiber.current

              http = EM::HttpRequest.new(url).get if verb == "get"
              http = EM::HttpRequest.new(url).post(:head => header, :body => body) if verb == "post"
              http = EM::HttpRequest.new(url).delete(:head => header) if verb == "delete"

              http.callback { f.resume(http) }
              http.errback { f.resume(http) }

              Fiber.yield

              [http.response_header.status, http.response]
            else
              response = @access_token.get(url) if verb =="get"
              response = @access_token.post(url, body, header) if verb == "post"
              response = @access_token.delete(url, header) if verb == "delete"
              [Integer(response.code), response.body]
            end
          end


          def load_catalog
            app_direct_catalog.current_offerings(offering_whitelist)
          end

          def purchase_service(order)
            # TODO: Order needs to include UUID for User, Organization, AppSpace
            if order
              body = order.to_json
              url = "#{@appdirect_endpoint}/api/#{SERVICES_PATH}"
              logger.info("Posting provision request: #{url}")
              logger.debug("Posted body: #{body}")

              http_status, response_body = perform_request("post", url, HEADER, body)

              if http_status >= 200 and http_status < 300
                logger.info("Provision successful")
                logger.debug("Body: #{response_body}")
                JSON.parse(response_body)
              else
                # 400 bad request
                # 500 if AppDirect has issues
                # 503 if ISV is down
                logger.error("Failed to provision: #{body} due to: #{http_status} - #{response_body}")
                raise AppdirectError.new(AppdirectError::APPDIRECT_ERROR_PURCHASE, http_status)
              end
            else
              raise ServiceError.new(ServiceError::INTERNAL_ERROR, "Missing order - cannot perform operation")
            end
          end

          def bind_service(order_id)
            if order_id
              body = {}.to_json
              url = "#{@appdirect_endpoint}/api/#{SERVICES_PATH}/#{order_id}/bindings"
              logger.info("Posting bind request: #{url}")

              http_status, response_body = perform_request("post", url, HEADER, body)

              if http_status >= 200 and http_status < 300
                logger.info("Bind successful: #{order_id}")
                JSON.parse(response_body)
              else
                logger.error("Bind request #{body} failed due to: #{http_status} - #{response_body}")
                raise AppdirectError.new(AppdirectError::APPDIRECT_ERROR_BIND, http_status)
              end
            else
              raise ServiceError.new(ServiceError::INTERNAL_ERROR, "Order and Order Id are required to bind to a service")
            end
          end

          def unbind_service(order_id, binding_id)
            if binding_id and order_id
              url = "#{@appdirect_endpoint}/api/#{SERVICES_PATH}/#{order_id}/bindings/#{binding_id}"
              logger.info("Unbind request: #{url}")

              http_status, response_body = perform_request("delete", url, HEADER, nil)

              if http_status >= 200 and http_status < 300
                logger.info("Unbind success for: OrderID: #{order_id}, BindingId: #{binding_id}")
              else
                logger.error("Failed to unbind service (id=#{order_id}): #{http_status} - #{response_body}")
                raise AppdirectError.new(AppdirectError::APPDIRECT_ERROR_UNBIND, http_status)
              end
            else
              raise ServiceError.new(ServiceError::INTERNAL_ERROR, "Binding Id and Order Id are required to cancel a service")
            end
          end

          def cancel_service(order_id)
            if order_id
              url = "#{@appdirect_endpoint}/api/#{SERVICES_PATH}/#{order_id}"
              logger.info("Unprovision request: #{url}")

              http_status, response_body = perform_request("delete", url, HEADER, nil)

              if http_status >= 200 and http_status < 300
                logger.info("Unprovision success for order id: #{order_id}")
              else
                logger.error("Failed to unprovision service (id=#{order_id}): #{http_status} - #{response_body}")
                raise AppdirectError.new(AppdirectError::APPDIRECT_ERROR_CANCEL, http_status)
              end
            else
              raise ServiceError.new(ServiceError::INTERNAL_ERROR, "Order Id is required to cancel a service")
            end
          end
        end
      end
    end
  end
end
