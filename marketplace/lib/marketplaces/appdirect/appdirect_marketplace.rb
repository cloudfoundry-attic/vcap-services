# Copyright (c) 2009-2012 VMware, Inc.
require 'fiber'
require 'service_error'
require_relative 'appdirect_helper'
require_relative 'name_and_provider_resolver'
require_relative 'appdirect_error'

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..')
require 'base/marketplace_base'

module VCAP
  module Services
    module Marketplace
      module Appdirect

        class AppdirectMarketplace < VCAP::Services::Marketplace::Base
          include VCAP::Services::Base::Error
          include VCAP::Services::Marketplace::Appdirect

          attr_reader :helper, :name_and_provider_resolver

          def initialize(opts)
            super(opts)
            @logger       = opts[:logger]
            @external_uri = opts[:external_uri]
            @node_timeout = opts[:node_timeout]
            @acls         = opts[:acls]
            @helper       = AppdirectHelper.new(opts, @logger)
            @mapping      = opts[:offering_mapping] || {}

            @cc_api_version = opts[:cc_api_version]
            @notification_email_domain   = opts[:notification_email_domain]

            @name_and_provider_resolver = NameAndProviderResolver.new(@mapping)
          end

          def name
            "AppDirect"
          end


          def get_catalog
            appdirect_catalog = helper.load_catalog
            catalog = {}
            appdirect_catalog.map(&:to_hash).each do |s|
              name, provider = name_and_provider_resolver.resolve_from_appdirect_to_cc(s.fetch('label'), s.fetch('provider'))
              version = s["version"] || "1.0" # UNTIL AD fixes this...
              key = key_for_service(name, version, provider)
              # Setup plans
              plans = {}
              if s.fetch("plans") and s.fetch("plans").count > 0
                s.fetch("plans").each do |plan|
                  plans[plan.fetch("id")] = {
                    unique_id: plan.fetch('external_id'),
                    description: plan.fetch("description"),
                    free: plan.fetch("free"),
                    extra: plan.fetch("extra") ? plan["extra"].to_json : nil
                  }
                end
              end

              # Finally, generate the catalog entry
              catalog[key] = {
                "unique_id"   => s.fetch("external_id"),
                "id"          => name,
                "version"     => version,
                "description" => s["description"] || "No description",
                "info_url"    => s.fetch("info_url"),
                "plans"       => plans,
                "provider"    => provider,
                "acls"        => @acls,
                "url"         => @external_uri,
                "timeout"     => @node_timeout,
                "extra"       => s.fetch("extra") ? s["extra"].to_json : nil,
                "tags"        => [], # unused in ccng, in cc a non-null value to allow tags clone during bind
              }
            end
            catalog
          end

          ##### Handle the 4 operations #####

          def provision_service(request_body)
            request =  VCAP::Services::Api::GatewayProvisionRequest.decode(request_body)
            id, _, version = request.label.rpartition("-")
            @logger.debug("Provision request for offering: #{request.label} (id=#{id}) provider=#{request.provider}, plan=#{request.plan}, version=#{request.version}")

            name, provider = name_and_provider_resolver.resolve_from_cc_to_appdirect(id, request.provider)
            email = "#{request.space_guid}@#{@notification_email_domain}"

            receipt = @helper.purchase_service(
              "space" => {
                "uuid"  => request.space_guid,
                "organization" => {
                  "uuid" => request.organization_guid,
                },
                "email" => email,
              },
              "offering" => {
                "label"    => name,
                "provider" => provider,
              },
              "configuration" => {
                "plan" => {
                  "external_id" => request.unique_id,
                },
                "name" => request.name,
              }
            )

            @logger.debug("AppDirect service provisioned #{receipt.inspect}")
            credentials = receipt["credentials"] || {}
            credentials["name"] = receipt["id"] #id of service within the 3rd party ISV
            #We could store more info in credentials but these will never be used by apps or users
            {
              :configuration => {:plan => request.plan, :name => request.name, :options => {} },
              :credentials => credentials,
              :service_id => receipt.fetch("uuid"),
              :dashboard_url => receipt.fetch('dashboard_url'),
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
