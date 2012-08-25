# Copyright (c) 2009-2012 VMware, Inc.
require 'fiber'
require 'dm-types'
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
            @url          = opts[:url]
            @node_timeout = opts[:node_timeout]
            @acls         = opts[:acls]
            @helper       = AppdirectHelper.new(opts, @logger)
          end

          def name
            "AppDirect"
          end

          def get_catalog
            @helper.get_catalog
          end

          def generate_cc_advertise_request(name, bsvc, active = true)
            req = {}
            req[:label] = "#{name}-#{bsvc["version"]}"
            req[:active] = active && bsvc["active"]
            req[:description] = bsvc["description"]

            req[:supported_versions] = [ bsvc["version"] ]
            req[:version_aliases]    =  { "current" => bsvc["version"] }

            if bsvc["developers"] and bsvc["developers"].count > 0
              acls = []
              bsvc["developers"].each do |dev|
                acls << dev["email"]
              end
              req[:acls] = {}
              req[:acls][:wildcards] = @acls
              req[:acls][:users] = acls
            end

            req[:url] = @url

            if bsvc["plans"] and bsvc["plans"].count > 0
              req[:plans] = []
              bsvc["plans"].each do |plan|
                req[:plans] << plan["id"]
                # No plan options yet
              end
            else
              req[:plans] = ["default"]
            end

            req[:tags] = ["default"] # No tags coming from AppDirect yet
            req[:timeout] = 5 + @node_timeout
            req
          end

          def provision_service(request_body)
            request =  VCAP::Services::Api::GatewayProvisionRequest.decode(request_body)
            @logger.info("Provision request for label=#{request.label} plan=#{request.plan}, version=#{request.version}")

            id,version = request.label.split("-")

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
        end
      end
    end
  end
end
