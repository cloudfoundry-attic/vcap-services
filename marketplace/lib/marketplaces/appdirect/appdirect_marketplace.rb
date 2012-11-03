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

          def generate_cc_advertise_request(name, ad_svc_offering, active = true)
            if (@mapping.keys.include?(name.to_sym))
              service_mapping = @mapping[name.to_sym]
              name = service_mapping[:name]
              provider = service_mapping[:provider]
            else
              # We'll use the service name as provider unless appdirect sends otherwise
              provider = ad_svc_offering["provider"] || name
            end

            req = {}
            req[:label] = "#{name}-#{ad_svc_offering["version"]}"
            req[:active] = active && ad_svc_offering["active"]
            req[:description] = ad_svc_offering["description"]

            req[:provider] = provider

            req[:supported_versions] = [ ad_svc_offering["version"] ]
            req[:version_aliases]    =  { "current" => ad_svc_offering["version"] }

            req[:url] = @external_uri

            wildcards = @acls[:wildcards] if @acls

            users_from_config = @acls[:users] if @acls
            ad_devs = ad_svc_offering["developers"] if ad_svc_offering["development"]
            ad_dev_emails = ad_devs.map { |u| u["email"] } if ad_devs
            users = (users_from_config || []) + (ad_dev_emails || []) if users_from_config || ad_dev_emails

            req[:acls] = {} if users || wildcards
            req[:acls][:users] = users if users
            req[:acls][:wildcards] = wildcards if wildcards

            if ad_svc_offering["plans"] and ad_svc_offering["plans"].count > 0
              req[:plans] = []
              ad_svc_offering["plans"].each do |plan|
                req[:plans] << plan["id"]
                # No plan options yet
              end
            else
              req[:plans] = ["default"]
            end

            req[:tags] = [] # A non-null value to allow tags clone during bind
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
