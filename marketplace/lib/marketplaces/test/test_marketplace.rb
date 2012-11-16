# Copyright (c) 2009-2012 VMware, Inc.
require 'fiber'
require 'service_error'
require 'uuidtools'

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..')
require 'base/marketplace_base'

module VCAP
  module Services
    module Marketplace
      module Test
        class TestMarketplace < VCAP::Services::Marketplace::Base

          include VCAP::Services::Base::Error

          def initialize(opts)
            super(opts)

            @logger       = opts[:logger]
            @external_uri = opts[:external_uri]
            @node_timeout = opts[:node_timeout]
            @acls         = opts[:acls]

            @cc_api_version = opts[:cc_api_version]

            testservice = {
              "id" => "testservice",
              "version" => "1.0",
              "name" => "My Test Service",
              "description" => "My test Service",
              "plans" => [ "free" ],
              "provider" => "TestProvider"
            }

            @catalog = {}
            @catalog[key_for_service(testservice["id"], testservice["version"], testservice["provider"])] = testservice

            @runtime_config = {}
            @runtime_config[:sleep_before_provision] = 0
          end

          def set_config(key, value)
            if key == "enable_foo"
              fooservice = {
                "id" => "fooservice",
                "version" => "1.0",
                "name" => "Foo Service",
                "description" => "Foo Service",
                "plans" => [ "free" ],
                "provider" => "FooProvider"
              }

              service_key = key_for_service(fooservice["id"], fooservice["version"], fooservice["provider"])

              if value == "true"
                @logger.info("Enabling fooservice-1.0")
                @catalog[service_key] = fooservice
              else
                @logger.info("Disabling fooservice-1.0")
                @catalog.delete(service_key)
              end

              @logger.info("Catalog contains #{@catalog.size} offerings")

            elsif key == "sleep_before_provision"
              @runtime_config[:sleep_before_provision] = Integer(value)
              @logger.info("sleep_before_provision = #{@runtime_config[:sleep_before_provision]}")
            end
          end

          def name
            "Test"
          end

          def get_catalog
            @catalog
          end

          def offering_disabled?(id, offerings_list)
            @logger.info("Offering: #{id} - Present in offering list: #{offerings_list.include?(id)}")
            !(offerings_list.include?(id))
          end

          def generate_cc_advertise_request(svc, active = true)
            req = {}
            req[:label] = "#{svc["id"]}-#{svc["version"]}"
            req[:active] = active
            req[:description] = svc["description"]
            req[:provider] = svc["provider"]

            req[:supported_versions] = [ svc["version"] ]
            req[:version_aliases]    =  { "current" => svc["version"] }

            req[:acls] = @acls
            req[:url] = @external_uri
            req[:plans] = svc["plans"]
            req[:tags] = []
            req[:timeout] = 5 + @node_timeout
            req
          end

          def generate_ccng_advertise_request(svc, active = true)
            req = {}
            req[:label] = svc["id"]
            req[:version] = svc["version"]
            req[:active] = active
            req[:description] = svc["description"]
            req[:provider] = svc["provider"]

            req[:acls] = @acls
            req[:url] = @external_uri
            req[:timeout] = 5 + @node_timeout

            # req[:supported_versions] = [ svc["version"] ]
            # req[:version_aliases]    =  { "current" => svc["version"] }

            plans = {}
            svc["plans"].each { |p| plans[p] = { "name" => p, "description" => "#{p} plan"} }

            [ req, plans ]
          end

          def provision_service(request_body)
            if @runtime_config[:sleep_before_provision] > 0
              @logger.info("Sleep before provision is set to: #{@runtime_config[:sleep_before_provision]} sec, Sleeping...")
              sleep @runtime_config[:sleep_before_provision]
            end

            request =  VCAP::Services::Api::GatewayProvisionRequest.decode(request_body)
            service_id,version = request.label.split("-")
            @logger.info("Provision request for label=#{request.label} (service_id=#{service_id}) plan=#{request.plan}, version=#{request.version}")
            {
              :configuration => {:plan => request.plan, :name => request.name, :options => {} },
              :credentials => { "url" => "http://testservice.com/#{UUIDTools::UUID.random_create.to_s}" },
              :service_id => UUIDTools::UUID.random_create.to_s,
            }
          end

          def unprovision_service(service_id)
            @logger.info("Successfully unprovisioned service #{service_id}")
          end

          def bind_service_instance(service_id, request)
            binding = {
              :configuration => {:data => {:binding_options => request.binding_options}},
              :credentials => { "url" => "http://testservice.com/#{UUIDTools::UUID.random_create.to_s}" },
              :service_id => UUIDTools::UUID.random_create.to_s
            }
            @logger.debug("Generated binding for CC: #{binding.inspect}")
            binding
          end

          def unbind_service(service_id, binding_id)
            @logger.info("Successfully unbound service #{service_id} and binding id #{binding_id}")
          end

          def varz_details
            { :available_services => @catalog.size }
          end

          def fmt_error(e)
            "#{e} [#{e.backtrace.join("|")}]"
          end

        end
      end
    end
  end
end
