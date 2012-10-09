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

            @catalog = {}
            @catalog["testservice-1.0"] =
            {
              "id" => "testservice",
              "version" => "1.0",
              "name" => "My Test Service",
              "description" => "My test Service",
              "plans" => [ { "id" => "free", "name" => "free edition", "description" => "for demo purposes only" } ],
              "development" => true,
              "developers" => [ { "email" => "foo@xyz.com" } ],
              "active" => true,
              "provider" => "TestProvider"
            }

            @runtime_config = {}
            @runtime_config[:sleep_before_provision] = 0
          end

          def set_config(key, value)
            if key == "enable_foo"
              if value == "true"
                @logger.info("Enabling fooservice-1.0")
                @catalog["fooservice-1.0"] =
                {
                  "id" => "fooservice",
                  "version" => "1.0",
                  "name" => "Foo Service",
                  "description" => "Foo Service",
                  "plans" => [ { "id" => "free", "name" => "free edition", "description" => "for demo purposes only" } ],
                  "development" => true,
                  "developers" => [ { "email" => "foo@xyz.com" } ],
                  "active" => true,
                  "provider" => "FooProvider"
                }
              else
                @logger.info("Disabling fooservice-1.0")
                @catalog.delete("fooservice-1.0")
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

          def generate_cc_advertise_request(name, bsvc, active = true)
            req = {}
            req[:label] = "#{name}-#{bsvc["version"]}"
            req[:active] = active && bsvc["active"]
            req[:description] = bsvc["description"]

            req[:provider] = bsvc["provider"]

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

            req[:tags] = []

            req[:timeout] = 5 + @node_timeout
            req
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
