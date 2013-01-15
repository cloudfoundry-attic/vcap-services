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
              "label" => "testservice-1.0",
              "name" => "My Test Service",
              "description" => "My test Service",
              "plans" => [ "100", "free" ],
              "default_plan" => "100",
              "provider" => "TestProvider",
              "supported_versions" => ["1.0"],
              "version_aliases" => { "current" => "1.0" },
              "timeout" => @node_timeout,
              "url" => @external_uri
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
                "label" => "fooservice-1.0",
                "name" => "Foo Service",
                "description" => "Foo Service",
                "plans" => [ "free" ],
                "default_plan" => "free",
                "provider" => "FooProvider",
                "supported_versions" => ["1.0"],
                "version_aliases" => { "current" => "1.0" },
                "timeout" => @node_timeout,
                "url" => @external_uri
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

          def provision_service(request_body)
            if @runtime_config[:sleep_before_provision] > 0
              @logger.info("Sleep before provision is set to: #{@runtime_config[:sleep_before_provision]} sec, Sleeping...")
              sleep @runtime_config[:sleep_before_provision]
            end

            request =  VCAP::Services::Api::GatewayProvisionRequest.decode(request_body)
            service_id,version = request.label.split("-")
            @logger.info("Provision request: #{request.inspect} - for label=#{request.label} (service_id=#{service_id}) plan=#{request.plan}, version=#{request.version}")
            {
              :configuration => {:plan => request.plan, :name => request.name, :options => {} },
              :credentials => { "url" => "http://testservice.com/#{UUIDTools::UUID.random_create.to_s}" },
              :service_id => UUIDTools::UUID.random_create.to_s,
            }
          end

          def unprovision_service(service_id)
            @logger.info("Successfully unprovisioned service #{service_id}")
          end

          def bind_service_instance(service_id, binding_options)
            binding = {
              :configuration => {:data => {:binding_options => binding_options}},
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
