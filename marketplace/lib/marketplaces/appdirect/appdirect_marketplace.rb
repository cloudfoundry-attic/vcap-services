# Copyright (c) 2009-2012 VMware, Inc.
require 'fiber'
require 'service_error'

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..')
require 'base/marketplace_base'

module VCAP
  module Services
    module Marketplace
      module Appdirect
        class AppdirectMarketplace < VCAP::Services::Marketplace::Base

          include VCAP::Services::Base::Error
          include VCAP::Services::Marketplace::Appdirect

          def initialize(opts)
            super(opts)

            @logger       = opts[:logger]
            @external_uri = opts[:external_uri]
            @node_timeout = opts[:node_timeout]
            @acls         = opts[:acls]
            @helper       = AppdirectHelper.new(opts, @logger)
            @mapping      = opts[:offering_mapping] || {}

            # Maintain a reverse mapping since we'll be changing the service name for CC advertisement
            # A provision request will require the actual service name rather than the one in CCDB
            @service_id_map = {}
            @mapping.keys.each { |k|
              service_name = @mapping[k.to_sym][:name]
              @service_id_map[service_name] = k.to_s
            }
          end

          def name
            "AppDirect"
          end

          def get_catalog
            @helper.get_catalog
          end

          def generate_cc_advertise_request(name, bsvc, active = true)

            if (@mapping.keys.include?(name.to_sym))
              service_mapping = @mapping[name.to_sym]
              name = service_mapping[:name]
              provider = service_mapping[:provider]
            else
              # We'll use the service name as provider unless appdirect sends otherwise
              provider = bsvc["provider"] || name
            end

            req = {}
            req[:label] = "#{name}-#{bsvc["version"]}"
            req[:active] = active && bsvc["active"]
            req[:description] = bsvc["description"]

            req[:provider] = provider

            req[:supported_versions] = [ bsvc["version"] ]
            req[:version_aliases]    =  { "current" => bsvc["version"] }

            req[:acls] = {}
            req[:acls][:wildcards] = @acls[:wildcards]

            users = []
            users.concat(@acls[:users].dup) if @acls[:users]
            if bsvc["developers"] and bsvc["developers"].count > 0
              bsvc["developers"].each do |dev|
                users << dev["email"]
              end
            end
            req[:acls][:users] = users unless users.empty?

            req[:url] = @external_uri

            if bsvc["plans"] and bsvc["plans"].count > 0
              req[:plans] = []
              bsvc["plans"].each do |plan|
                req[:plans] << plan["id"]
                # No plan options yet
              end
            else
              req[:plans] = ["default"]
            end

            req[:timeout] = 5 + @node_timeout
            req
          end

          def offering_disabled?(id, offerings_list)
            # Translate service name if a custom mapping was defined
            id = @service_id_map[id] if @service_id_map.keys.include?(id)

            # Check if its still listed
            @logger.info("Offering: #{id} - Present in offering list: #{offerings_list.include?(id)}")
            !(offerings_list.include?(id))
          end

          def provision_service(request_body)
            request =  VCAP::Services::Api::GatewayProvisionRequest.decode(request_body)
            id,version = request.label.split("-")
            id = @service_id_map[id] if @service_id_map.keys.include?(id)
            @logger.debug("Provision request for label=#{request.label} (id=#{id}) plan=#{request.plan}, version=#{request.version}")

            order = {
              "user" => {
                "uuid" => nil,
                "email" => request.email
              },
              "offering" => {
                "id" => id,
                "version" => request.version || version
              },
              "configuration" => {
                "plan" => request.plan,
                "name" => request.name,
                "options" => {}
              }
            }
            receipt = @helper.purchase_service(order)

            if receipt
              @logger.debug("AppDirect service provisioned #{receipt.inspect}")
              credentials = receipt["credentials"] || {}
              credentials["name"] = receipt["id"] #id of service within the 3rd party ISV
              #We could store more info in credentials but these will never be used by apps or users
              svc = {
                :configuration => {:plan => request.plan, :name => request.name, :options => {} },
                :credentials => credentials,
                :service_id => receipt["uuid"],
              }
              success(svc)
            else
              @logger.warn("Invalid response to provision service label=#{request.label}")
              raise ServiceError.new(ServiceError::INTERNAL_ERROR, "Missing request -- cannot perform operation")
            end
          rescue => e
            if e.instance_of? ServiceError
              failure(e)
            else
              @logger.debug(e.inspect)
              @logger.warn("Can't provision service label=#{request.label}: #{fmt_error(e)}")
              internal_fail
            end
          end

          def unprovision_service(service_id)
            success = @helper.cancel_service(service_id)
            if success
             @logger.info("Successfully unprovisioned service #{service_id}")
            else
              @logger.info("Failed to unprovision service #{service_id}")
            end
            return success
          rescue => e
            if e.instance_of? ServiceError
              failure(e)
            else
              @logger.warn("Can't unprovision service service_id=#{service_id}: #{fmt_error(e)}")
              internal_fail
            end
          end

          def bind_service_instance(service_id, request)
            if service_id and request
              order = {
                "options" => request.binding_options
              }
              resp = @helper.bind_service(order, service_id)
              @logger.debug("Got response from AppDirect: #{resp.inspect}")
              binding = {
                :configuration => {:data => {:binding_options => request.binding_options}},
                :credentials => resp["credentials"],
                :service_id => resp["uuid"],  #Important this is the binding_id
              }
              @logger.debug("Generated binding for CC: #{binding.inspect}")
              success(binding)
            else
              @logger.warn("Can't find service label=#{label}")
              raise ServiceError.new(ServiceError::INTERNAL_ERROR, "Missing request or service_id -- cannot perform operation")
            end
          rescue => e
            if e.instance_of? ServiceError
              failure(e)
            else
              @logger.warn("Can't bind service service_id=#{service_id}, request=#{request}: #{fmt_error(e)}")
              internal_fail
            end
          end

          def unbind_service(service_id, binding_id)
            success = @helper.unbind_service(service_id, binding_id)
            begin
              if success
                @logger.info("Successfully unbound service #{service_id} and binding id #{binding_id}")
              else
                @logger.info("Failed to unbind service #{service_id} and binding id #{binding_id}")
              end
              return success
            rescue => e
              if e.instance_of? ServiceError
                failure(e)
              else
                @logger.warn("Can't unprovision service service_id=#{service_id}: #{fmt_error(e)}")
                internal_fail
              end
            end
          end

          def fmt_error(e)
            "#{e} [#{e.backtrace.join("|")}]"
          end

        end
      end
    end
  end
end
