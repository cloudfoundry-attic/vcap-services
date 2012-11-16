require 'fiber'
require 'nats/client'
require 'uri'


module VCAP
  module Services
    module Marketplace
      class CloudControllerClient

          REQ_OPTS = %w(cloud_controller_uri cc_req_hdrs marketplace_client_name logger).map {|o| o.to_sym}

          def initialize(opts)
            missing_opts = REQ_OPTS.select {|o| !opts.has_key? o}
            raise ArgumentError, "Missing options: #{missing_opts.join(', ')}" unless missing_opts.empty?

            cld_ctrl_uri             = opts[:cloud_controller_uri]
            @service_list_uri        = "#{cld_ctrl_uri}/proxied_services/v1/offerings"
            @offering_uri            = "#{cld_ctrl_uri}/services/v1/offerings"
            @cc_req_hdrs             = opts[:cc_req_hdrs]
            @marketplace_client_name = opts[:marketplace_client_name]
            @logger                  = opts[:logger]

            @proxy_opts              = opts[:proxy]
          end

          def create_http_request(args)
            req = {
             :head => args[:head],
             :body => args[:body],
           }

           if (@proxy_opts)
             req[:proxy] = @proxy_opts
             # this is a workaround for em-http-requesr 0.3.0 so that headers are not lost
             # more info: https://github.com/igrigorik/em-http-request/issues/130
             req[:proxy][:head] = req[:head]
           end

           req
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
                resp = JSON.parse(http.response)
                resp["proxied_services"].each {|bsvc|
                  @logger.info("Fetch #{@marketplace_client_name} service from CC: label=#{bsvc["label"]} - #{bsvc.inspect}")
                  services[bsvc["label"]] = bsvc
                }
              else
                raise "Failed to fetch #{@marketplace_client_name} service from CC - status=#{http.response_header.status}"
              end
            else
              raise "Failed to fetch #{@marketplace_client_name} service from CC: #{http.error}"
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
                return true
              else
                @logger.warn("Failed advertise offerings:#{offering.inspect}, status=#{http.response_header.status}")
              end
            else
              @logger.warn("Failed advertise offerings:#{offering.inspect}: #{http.error}")
            end

            return false
          end
      end
    end
  end
end
