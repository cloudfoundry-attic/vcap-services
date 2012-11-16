require 'fiber'
require 'nats/client'
require 'uri'
require 'uaa/token_issuer'
require 'services/api/const'


module VCAP
  module Services
    module Marketplace
      class CCNGClient

          REQ_OPTS = %w(cloud_controller_uri service_auth_tokens uaa_endpoint uaa_client_auth_credentials marketplace_client_name logger).map {|o| o.to_sym}

          def initialize(opts)
            missing_opts = REQ_OPTS.select {|o| !opts.has_key? o}
            raise ArgumentError, "Missing options: #{missing_opts.join(', ')}" unless missing_opts.empty?

            @cld_ctrl_uri            = opts[:cloud_controller_uri]

            @service_list_uri        = "#{@cld_ctrl_uri}/v2/services?inline-relations-depth=2"
            @offering_uri            = "#{@cld_ctrl_uri}/v2/services"
            @service_plans_uri       = "#{@cld_ctrl_uri}/v2/service_plans"
            @marketplace_client_name = opts[:marketplace_client_name]
            @logger                  = opts[:logger]
            @service_auth_tokens     = opts[:service_auth_tokens]

            @proxy_opts              = opts[:proxy]

            # Load the auth token to be sent out in Authorization header when making CCNG-v2 requests
            uaa_client_auth_credentials = opts[:uaa_client_auth_credentials]
            ti = CF::UAA::TokenIssuer.new(opts[:uaa_endpoint], "vmc", "", nil)
            token = ti.implicit_grant_with_creds(uaa_client_auth_credentials, nil).info

            @uaa_client_auth_token = "#{token[:token_type]} #{token[:access_token]}"
            @logger.info("Successfully retrieved auth token for: #{uaa_client_auth_credentials[:username]}")
            @logger.info("Service tokens: #{@service_auth_tokens.inspect}")
          end

          def create_http_request(body = nil)
            req = {
             :head => { 'Content-Type' => 'application/json', 'Authorization' => @uaa_client_auth_token },
             :body => body
           }

           if (@proxy_opts)
             req[:proxy] = @proxy_opts
             # this is a workaround for em-http-requesr 0.3.0 so that headers are not lost
             # more info: https://github.com/igrigorik/em-http-request/issues/130
             req[:proxy][:head] = req[:head]
           end

           req
          end

          def get_registered_services_from_cc
            @logger.debug("Getting services listing from cloud_controller: #{@service_list_uri}")
            registered_services = {}
            svcs = get_services

            svcs["resources"].each do |s|
              key = "#{s["entity"]["label"]}_#{s["entity"]["provider"]}"

              if @service_auth_tokens.has_key?(key.to_sym)
                entity = s["entity"]
                svc = {
                  "label"       => entity["label"],
                  "description" => entity["description"],
                  "provider"    => entity["provider"],
                  "version"     => entity["version"],
                  "url"         => entity["url"],
                  "info_url"    => entity["info_url"]
                }

                plans = {}
                entity["service_plans"].each { |p|
                  plans[p["entity"]["name"]] = {
                    "guid"        => p["metadata"]["guid"],
                    "name"        => p["entity"]["name"],
                    "description" => p["entity"]["description"]
                  }
                }

                registered_services[key] = {
                  "guid"    => s["metadata"]["guid"],
                  "service" => svc,
                  "plans"   => plans,
                }

                @logger.debug("Found #{key} = #{registered_services[key].inspect}")
              end
            end

            registered_services
          end

          def get_services
            req = create_http_request

            f = Fiber.current
            http = EM::HttpRequest.new(@service_list_uri).get(req)
            http.callback { f.resume(http) }
            http.errback { f.resume(http) }
            Fiber.yield

            if http.error.empty?
              if http.response_header.status == 200
                return JSON.parse(http.response)
              else
                raise "Failed to fetch #{@marketplace_client_name} service from CC - status=#{http.response_header.status}"
              end
            else
              raise "Failed to fetch #{@marketplace_client_name} service from CC: #{http.error}"
            end

            nil
          end

          def advertise_service_to_cc(offering, guid, plans_to_add, plans_to_update)
            service_guid = advertise_service(offering, guid)
            return false if service_guid.nil?

            @logger.debug("Processing plans for: #{service_guid} -Add: #{plans_to_add.size} plans, Update: #{plans_to_update.size} plans")

            # Add plans to add
            plans_to_add.each { |plan|
              plan["service_guid"] = service_guid
              add_or_update_plan(plan)
            }

            # Update plans
            plans_to_update.each { |plan_guid, plan|
              add_or_update_plan(plan, plan_guid)
            }

            return true
          end

          def advertise_service(offering, guid)
            update = !guid.nil?
            uri = update ? "#{@offering_uri}/#{guid}" : @offering_uri

            @logger.debug("#{update ? "Update" : "Advertise"} service offering #{offering.inspect} to cloud_controller: #{uri}")

            req = create_http_request(Yajl::Encoder.encode(offering))

            f = Fiber.current
            conn = EM::HttpRequest.new(uri)

            http = update ? conn.put(req) : conn.post(req)
            http.callback { f.resume(http) }
            http.errback { f.resume(http) }

            Fiber.yield

            if http.error.empty?
              if (200..299) === http.response_header.status
                response = JSON.parse(http.response)
                @logger.info("Advertise offering response (code=#{http.response_header.status}): #{response.inspect}")
                return response["metadata"]["guid"]
              else
                @logger.warn("Failed advertise offerings:#{offering.inspect}, status=#{http.response_header.status}")
              end
            else
              @logger.warn("Failed advertise offerings:#{offering.inspect}: #{http.error}")
            end

            return nil
          end

          def add_or_update_plan(plan, plan_guid = nil)
            add_plan = plan_guid.nil?

            url = add_plan ? @service_plans_uri : "#{@service_plans_uri}/#{plan_guid}"
            @logger.info("#{add_plan ? "Add new plan" : "Update plan (guid: #{plan_guid}) to"}: #{plan.inspect} via #{url}")

            req = create_http_request(Yajl::Encoder.encode(plan))

            f = Fiber.current

            conn = EM::HttpRequest.new(url)
            http = add_plan ? conn.post(req) : conn.put(req)
            http.callback { f.resume(http) }
            http.errback { f.resume(http) }

            Fiber.yield

            if http.error.empty?
              if (200..299) === http.response_header.status
                @logger.info("Successfully #{add_plan ? "added" : "updated"} service plan: #{plan.inspect}")
                return true
              else
                @logger.warn("Failed to #{add_plan ? "add" : "update"} plan: #{plan.inspect}, status=#{http.response_header.status}")
              end
            else
              @logger.warn("Failed to #{add_plan ? "add" : "update"} plan: #{plan.inspect}: #{http.error}")
            end

            return false
          end
      end
    end
  end
end
