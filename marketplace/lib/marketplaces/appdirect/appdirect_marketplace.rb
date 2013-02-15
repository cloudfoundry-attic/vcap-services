# Copyright (c) 2009-2012 VMware, Inc.
require 'fiber'
require 'service_error'
require_relative 'appdirect_helper'

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
            @mapping.keys.each { |ad_key|
              cc_key = "#{@mapping[ad_key][:cc_name]}_#{@mapping[ad_key][:cc_provider]}".to_sym
              @service_id_map[cc_key] = ad_key
            }
          end

          def name
            "AppDirect"
          end

          def get_catalog
            appdirect_catalog = @helper.load_catalog
            catalog = {}
            appdirect_catalog.each { |s|
              key = "#{s["label"]}_#{s["provider"]}".to_sym
              raise "Mapping missing for whitelisted offering - label: #{s["label"]} / provider: #{s["provider"]}" unless @mapping.keys.include?(key)

              mapping  = @mapping[key]
              name     = mapping[:cc_name]
              provider = mapping[:cc_provider]

              version = s["version"] || "1.0" # UNTIL AD fixes this...
              key = key_for_service(name, version, provider)

              # Setup acls
              # TODO: Use per service offering acls
              acls = @acls

              # Setup plans
              plans = {}
              if s["plans"] and s["plans"].count > 0
                s["plans"].each do |plan|
                  plans[plan["id"]] = { :description => plan["description"], :free => plan["free"] }
                end
              end

              # Finally, generate the catalog entry
              catalog[key] = {
                "id"          => name,
                "version"     => version,
                "description" => s["description"] || "No description",
                "info_url"    => s["info_url"],
                "plans"       => plans,
                "provider"    => provider,
                "acls"        => acls,
                "url"         => @external_uri,
                "timeout"     => @node_timeout,
                "tags"        => [], # unused in ccng, in cc a non-null value to allow tags clone during bind
              }
            }
            catalog
          end

          ##### Handle the 4 operations #####

          def provision_service(request_body)
            request =  VCAP::Services::Api::GatewayProvisionRequest.decode(request_body)
            id,version = request.label.split("-")

            cc_key = "#{id}_#{request.provider}".to_sym
            raise "Mapping does not exist for: #{cc_key.to_s}" unless @service_id_map.keys.include?(cc_key)

            ad_key = @service_id_map[cc_key]
            raise "Requested mapping for unknown label: #{name} / provider: #{provider}" unless @mapping.keys.include?(ad_key)

            mapping  = @mapping[ad_key]
            name     = mapping[:ad_name]
            provider = mapping[:ad_provider]

            @logger.debug("Provision request for offering: #{request.label} (id=#{id}) provider=#{request.provider}, plan=#{request.plan}, version=#{request.version}")

            email = "#{request.space_guid}@cloudfoundry.com" # Generate fake email to allow AD to create accounts in ISV website
            order = {
              "space" => {
                "uuid"  => request.space_guid,
                "organization" => {
                  "uuid" => request.organization_guid
                },
                "email" => email
              },
              "offering" => {
                "label"    => name,
                "provider" => provider
              },
              "configuration" => {
                "plan" => request.plan,
                "name" => request.name,
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

          def bind_service_instance(service_id, binding_options)
            # NOTE: binding_options unused in interations with appdirect
            resp = @helper.bind_service(service_id)
            @logger.debug("Bind response from AppDirect: #{resp.inspect}")

            {
              :configuration => {:data => {:binding_options => binding_options}},
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
