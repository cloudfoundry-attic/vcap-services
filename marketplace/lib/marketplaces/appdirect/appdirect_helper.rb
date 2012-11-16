# Copyright (c) 2009-2012 VMware, Inc.
require "oauth"
require "json"
require "fiber"

$:.unshift(File.dirname(__FILE__))
require "appdirect_error"

module VCAP
  module Services
    module Marketplace
      module Appdirect
          class AppdirectHelper

            OFFERINGS_PATH = "custom/cloudfoundry/v1/offerings"
            SERVICES_PATH = "custom/cloudfoundry/v1/services"

            HEADER = {"Content-Type" => "application/json" , "Accept"=>"application/json"}
            REQ_CONFIG = %w(endpoint key secret).map {|o| o.to_sym}

            def initialize(opts, logger)
              @logger = logger

              raise("No appdirect config section provided in: #{opts.inspect}") unless opts[:appdirect]

              appdirect_config = opts[:appdirect]

              missing_opts = REQ_CONFIG.select {|o| !appdirect_config.has_key? o}
              raise ArgumentError, "Missing options: #{missing_opts.join(', ')}" unless missing_opts.empty?

              @appdirect_endpoint = appdirect_config[:endpoint]
              @whitelist          = opts[:offering_whitelist]

              @consumer = OAuth::Consumer.new(appdirect_config[:key], appdirect_config[:secret])
              @access_token = OAuth::AccessToken.new(@consumer)
            end

            def load_catalog
              url = "#{@appdirect_endpoint}/api/#{OFFERINGS_PATH}"
              @logger.debug("Getting service listing from: #{url}")

              response = @access_token.get(url)
              http_status = Integer(response.code)
              if http_status == 200
                data = JSON.parse(response.body) #VCAP::Services::AppDirect::AppDirectCatalogResponse.decode(raw)
                catalog = []
                data.each do |service|
                  # Add checks for specific categories which determine whether the addon should be listed on cc
                  @logger.debug("Got service '#{service["id"]}' from AppDirect")
                  if (@whitelist.nil? || @whitelist.include?(service["id"]))
                    catalog << service
                  else
                    @logger.warn("Ignoring service Offering: #{service["id"]} since it is not whitelisted")
                  end
                end
                @logger.info("Got #{catalog.size} services from AppDirect")
                return catalog
              else
                @logger.error("Failed to get catalog #{http_status}")
                raise AppdirectError.new(AppdirectError::APPDIRECT_ERROR_GET_LISTING, http_status)
              end
            end

            def purchase_service(order)
              # TODO: Order needs to include UUID for User, Organization, AppSpace
              if order
                body = order.to_json
                url = "#{@appdirect_endpoint}/api/#{SERVICES_PATH}"
                @logger.info("Posting provision request: #{url}")

                response = @access_token.post(url, body, HEADER)
                http_status = Integer(response.code)
                if http_status >= 200 and http_status < 300
                  @logger.info("Provision successful")
                  JSON.parse(response.body)
                else
                  # 400 bad request
                  # 500 if AppDirect has issues
                  # 503 if ISV is down
                  @logger.error("Failed to provision: #{body} due to: #{http_status} - #{response.body}")
                  raise AppdirectError.new(AppdirectError::APPDIRECT_ERROR_PURCHASE, http_status)
                end
              else
                raise ServiceError.new(ServiceError::INTERNAL_ERROR, "Missing order - cannot perform operation")
              end
            end

            def bind_service(order, order_id)
              if order and order_id
                body = order.to_json
                url = "#{@appdirect_endpoint}/api/#{SERVICES_PATH}/#{order_id}/bindings"
                @logger.info("Posting bind request: #{url}")

                response = @access_token.post(url, body, HEADER)
                http_status = Integer(response.code)
                if http_status >= 200 and http_status < 300
                  @logger.info("Bind successful: #{order_id}")
                  JSON.parse(response.body)
                else
                  @logger.error("Bind request #{body} failed due to: #{http_status} - #{response.body}")
                  raise AppdirectError.new(AppdirectError::APPDIRECT_ERROR_BIND, http_status)
                end
              else
                raise ServiceError.new(ServiceError::INTERNAL_ERROR, "Order and Order Id are required to bind to a service")
              end
            end

            def unbind_service(order_id, binding_id)
              if binding_id and order_id
                url = "#{@appdirect_endpoint}/api/#{SERVICES_PATH}/#{order_id}/bindings/#{binding_id}"
                @logger.info("Unbind request: #{url}")

                response = @access_token.delete(url, HEADER)
                http_status = Integer(response.code)
                if http_status >= 200 and http_status < 300
                  @logger.info("Unbind success for: OrderID: #{order_id}, BindingId: #{binding_id}")
                else
                  @logger.error("Failed to unbind service (id=#{order_id}): #{http_status} - #{response.body}")
                  raise AppdirectError.new(AppdirectError::APPDIRECT_ERROR_UNBIND, http_status)
                end
              else
                raise ServiceError.new(ServiceError::INTERNAL_ERROR, "Binding Id and Order Id are required to cancel a service")
              end
            end

            def cancel_service(order_id)
              if order_id
                url = "#{@appdirect_endpoint}/api/#{SERVICES_PATH}/#{order_id}"
                @logger.info("Unprovision request: #{url}")

                response = @access_token.delete(url, HEADER)
                http_status = Integer(response.code)
                if http_status >= 200 and http_status < 300
                  @logger.info("Unprovision success for order id: #{order_id}")
                else
                  @logger.error("Failed to unprovision service (id=#{order_id}): #{http_status} - #{response.body}")
                  raise AppdirectError.new(AppdirectError::APPDIRECT_ERROR_CANCEL, http_status)
                end
              else
                raise ServiceError.new(ServiceError::INTERNAL_ERROR, "Order Id is required to cancel a service")
              end
            end

=begin
          private
            def check_http_error(http)
              if !http.error.empty?
                 raise "HTTP Error: #{http.error}"
              end
            end

            def get_catalog_response
              url = "#{@appdirect_endpoint}/api/#{OFFERINGS_PATH}"
              @logger.debug("About to get service listing from #{url}")
              f = Fiber.current
              conn = EventMachine::HttpRequest.new(url)

              http = conn.get(:head => HEADER)
              http.errback {f.resume(http)}
              http.callback {f.resume(http)}

              Fiber.yield

              check_http_error(http)
              http
            end

            def post_order(body)
              url = "#{@appdirect_endpoint}/api/#{SERVICES_PATH}"
              @logger.info("About to post #{url}")
              f = Fiber.current
              conn = EventMachine::HttpRequest.new(url)

              http = conn.post(:head => HEADER, :body => body)
              http.errback {f.resume(http)}
              http.callback {f.resume(http)}

              Fiber.yield

              check_http_error(http)
              http
            end

            def delete_order(order_id)
              url = "#{@appdirect_endpoint}/api/#{SERVICES_PATH}/#{order_id}"
              @logger.info("About to delete #{url}")
              f = Fiber.current
              conn = EventMachine::HttpRequest.new(url)

              http = conn.delete(:head => HEADER)
              http.errback {f.resume(http)}
              http.callback {f.resume(http)}

              Fiber.yield

              check_http_error(http)
              http
            end

            def post_bind_service(body, order_id)
              url = "#{@appdirect_endpoint}/api/#{SERVICES_PATH}/#{order_id}/bindings"
              @logger.info("About to post #{url}")
              f = Fiber.current
              conn = EventMachine::HttpRequest.new(url)

              http = conn.post(:head => HEADER, :body => body)
              http.errback {f.resume(http)}
              http.callback {f.resume(http)}

              Fiber.yield

              check_http_error(http)
              http
            end

            def delete_bind_service(order_id, binding_id)
              url = "#{@appdirect_endpoint}/api/#{SERVICES_PATH}/#{order_id}/bindings/#{binding_id}"
              @logger.info("About to delete binding #{url}")
              f = Fiber.current
              conn = EventMachine::HttpRequest.new(url)

              http = conn.delete(:head => HEADER)
              http.errback {f.resume(http)}
              http.callback {f.resume(http)}

              Fiber.yield

              check_http_error(http)
              http
            end
=end

        end
     end
    end
  end
end
