# Copyright (c) 2009-2012 VMware, Inc.
require "oauth"
require "json"
require "fiber"

module VCAP
  module Services
    module Marketplace
      module Appdirect
          class AppdirectHelper

            include VCAP::Services::Marketplace::Appdirect

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

              @consumer = OAuth::Consumer.new(appdirect_config[:key],  appdirect_config[:secret])
              @access_token = OAuth::AccessToken.new(@consumer)
            end

            def get_catalog
              catalog = nil
              http = get_catalog_response
              if http.error.empty?
                if http.response_header.http_status == 200
                    @logger.debug("Got catalog response #{http.response}")
                  data = JSON.parse(http.response) #VCAP::Services::AppDirect::AppDirectCatalogResponse.decode(raw)
                  catalog = {}
                  data.each do |service|
                    # Add checks for specific categories which determine whether the addon should be listed on cc
                    @logger.debug("Got service '#{service["id"]}' from AppDirect")
                    catalog[service["id"]] = service
                  end
                  @logger.info("Got #{catalog.keys.count} services from AppDirect")
                else
                  @logger.warn("Failed to get catalog #{http.response}")
                end
              else
                @logger.warn("Failed to get catalog: #{http.error}")
                raise AppdirectError.new(AppDirectError::APPDIRECT_ERROR_GET_LISTING, http.response_header.status)
              end
              return catalog
            end

            def purchase_service(order)
              new_serv = nil
              # TODO: Order needs to include UUID for User, Organization, AppSpace
              if order
                body = order.to_json
                http = post_order(body)
                if http.error.empty?
                  if http.response_header.status >= 200 and http.response_header.status < 300
                    new_serv = JSON.parse(http.response)
                    return new_serv
                  else
                    # 400 bad request
                    # 500 if AppDirect has issues
                    # 503 if ISV is down
                    @logger.warn("Bad status code posting #{body} was #{http.response}")
                    raise AppdirectError.new(AppDirectError::APPDIRECT_ERROR_PURCHASE, http.response_header.status)
                  end
                else
                  @logger.warn("Error raised: #{http.error}")
                  raise AppdirectError.new(AppDirectError::APPDIRECT_ERROR_PURCHASE, http.response_header.status)
                end
              else
                @logger.error("Order is required to purchase a service")
              end
              new_serv
            end

            def bind_service(order, order_id)
              update_serv = nil
              if order and order_id
                body = order.to_json
                http = post_bind_service(body, order_id)

                if http.error.empty?
                  if http.response_header.status >= 200 and http.response_header.status < 300
                    @logger.debug("Got http headers #{http.headers}")
                    update_serv = JSON.parse(http.response)
                    @logger.debug("Bound service #{order_id}")
                  else
                    raise AppdirectError.new(AppDirectError::APPDIRECT_ERROR_BIND, http.response_header.status)
                  end
                else
                  @logger.warn("Error raised: #{http.error}")
                  raise AppdirectError.new(AppDirectError::APPDIRECT_ERROR_BIND, http.response_header.status)
                end
              else
                @logger.error("Order and Order Id are required to cancel a service")
              end
              update_serv
            end

            def unbind_service(order_id, binding_id)
              update_binding = false
              if binding_id and order_id
                http = delete_bind_service(order_id, binding_id)

                if http.error.empty?
                  if http.response_header.status >= 200 and http.response_header.status < 300
                    update_binding = true
                  else
                    @logger.warn("Invalid status code returned: #{http.response_header.status}")
                    raise AppdirectError.new(AppDirectError::APPDIRECT_ERROR_UNBIND, http.response_header.status)
                  end
                else
                  @logger.warn("Error raised: #{http.error}")
                  raise AppdirectError.new(AppDirectError::APPDIRECT_ERROR_UNBIND, http.response_header.status)
                end
              else
                @logger.error("Binding Id and Order Id are required to cancel a service")
              end
              update_binding
            end

            def cancel_service(order_id)
              cancel_serv = false
              if order_id
                http = delete_order(order_id)
                if http.response_header.status >= 200 and http.response_header.status < 300
                  @logger.debug("Deleted #{order_id}")
                  cancel_serv = true
                else
                  @logger.warn("Invalid status code returned: #{http.response_header.status}")
                  raise AppdirectError.new(AppDirectError::APPDIRECT_ERROR_CANCEL, http.response_header.status)
                end
              else
                @logger.error("Order Id is required to cancel a service")
              end
              cancel_serv
            end

          private
            def get_catalog_response
              url = "#{@appdirect_endpoint}/api/#{OFFERINGS_PATH}"
              @logger.debug("About to get service listing from #{url}")
              f = Fiber.current
              http = EventMachine::HttpRequest.new(url).get(:head => HEADER)
              http.errback {f.resume(http)}
              http.callback {f.resume(http)}

              return Fiber.yield
            end


            def post_order(body)
              url = "#{@appdirect_endpoint}/api/#{SERVICES_PATH}"
              @logger.info("About to post #{url}")
              f = Fiber.current
              http = EventMachine::HttpRequest.new(url).post(:head => HEADER, :body => body)
              http.errback {f.resume(http)}
              http.callback {f.resume(http)}

              return Fiber.yield
            end


            def delete_order(order_id)
              url = "#{@appdirect_endpoint}/api/#{SERVICES_PATH}/#{order_id}"
              @logger.info("About to delete #{url}")
              f = Fiber.current
              http = EventMachine::HttpRequest.new(url).delete(:head => HEADER)
              http.errback {f.resume(http)}
              http.callback {f.resume(http)}

              return Fiber.yield
            end

            def post_bind_service(body, order_id)
              url = "#{@appdirect_endpoint}/api/#{SERVICES_PATH}/#{order_id}/bindings"
              @logger.info("About to post #{url}")
              f = Fiber.current
              http = EventMachine::HttpRequest.new(url).post(:head => HEADER, :body => body)
              http.errback {f.resume(http)}
              http.callback {f.resume(http)}

              return Fiber.yield
            end

            def delete_bind_service(order_id, binding_id)
              url = "#{@appdirect_endpoint}/api/#{SERVICES_PATH}/#{order_id}/bindings/#{binding_id}"
              @logger.info("About to delete binding #{url}")
              f = Fiber.current
              http = EventMachine::HttpRequest.new(url).delete(:head => HEADER)
              http.errback {f.resume(http)}
              http.callback {f.resume(http)}

              return Fiber.yield
            end

        end
      end
    end
  end
end
