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

            @cc_api_version = opts[:cc_api_version]

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
            appdirect_catalog = @helper.load_catalog
            catalog = {}
            appdirect_catalog.each { |s|
              name, provider = load_mapped_name_and_provider(s["name"], s["vendor"])

              # Mutate / Duplicate the offering keys for uniformity with ccdb keys
              s["id"]       = s["name"]
              s["info_url"] = s["infoUrl"]
              s["provider"] = provider

              s["version"] ||= "1.0"   # UNTIL AD fixes this...
              key = key_for_service(name, s["version"], provider)
              catalog[key] = s
            }

            catalog
          end

          def load_mapped_name_and_provider(name, provider)
             svc_label = name

             # We'll use the service id as provider unless appdirect sends otherwise
             svc_provider = provider || name
             if (@mapping.keys.include?(name.to_sym))
               service_mapping = @mapping[name.to_sym]
               svc_label = service_mapping[:name]
               svc_provider = service_mapping[:provider]
            end

            [svc_label, svc_provider]
          end

          def generate_cc_advertise_request(ad_svc_offering, active = true)
            raise "Unsupported"
          end

          def generate_ccng_advertise_request(ad_svc_offering, active = true)
            # service name can be in id (if ad_svc_offering is from marketplace) or label (if ad_svc_offering is from ccdb)
            # See marketplace/lib/base/marketplace_async_gateway_v2.rb:- advertise_services

            label = ad_svc_offering["id"] || ad_svc_offering["label"]

            req = {}
            req[:label]       = label
            req[:version]     = ad_svc_offering["version"]
            req[:description] = ad_svc_offering["description"]
            req[:info_url]    = ad_svc_offering["info_url"]
            req[:provider]    = ad_svc_offering["provider"]

            req[:active] = active

            req[:url]     = @external_uri
            req[:acls]    = @acls
            req[:timeout] = @node_timeout

            # Setup plans
            plans = {}
            ad_svc_offering["plans"].each do |plan|
              plans[plan["id"]] = { "name" => plan["id"], "description" => plan["description"] }
            end

            [ req, plans ]
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
                "uuid" => request.email, # TODO: replace with actual UUID from ccng
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

            @logger.debug("AppDirect service provisioned #{receipt.inspect}")
            credentials = receipt["credentials"] || {}
            credentials["name"] = receipt["id"] #id of service within the 3rd party ISV
            #We could store more info in credentials but these will never be used by apps or users
            {
              :configuration => {:plan => request.plan, :name => request.name, :options => {} },
              :credentials => credentials,
              :service_id => receipt["uuid"],
            }
          end

          def unprovision_service(service_id)
            @helper.cancel_service(service_id)
          end

          def bind_service_instance(service_id, request)
            order = {
              "options" => request.binding_options
            }
            resp = @helper.bind_service(order, service_id)
            @logger.debug("Bind response from AppDirect: #{resp.inspect}")
            {
              :configuration => {:data => {:binding_options => request.binding_options}},
              :credentials => resp["credentials"],
              :service_id => resp["uuid"],  #Important this is the binding_id
            }
          end

          def unbind_service(service_id, binding_id)
            @helper.unbind_service(service_id, binding_id)
          end

        end
      end
    end
  end
end
