# Copyright (c) 2009-2012 VMware, Inc.
module VCAP
  module Services
    module Marketplace
      class Base

        def initialize(opts_unused)
        end

        def name
          "UNKNOWN"
        end

        def get_catalog
          {}
        end


        # TODO: Merge these 2 functions...
        def generate_cc_advertise_request(svc, active = true)
          {}
        end

        def generate_ccng_advertise_request(svc, active = true)
          {}
        end

        def offering_disabled?(name, offerings)
          false
        end

        def provision_service(request_body)
        end

        def unprovision_service(service_id)
        end

        def bind_service_instance(service_id, request)
        end

        def unbind_service(service_id, binding_id)
        end

        def varz_details
          {}
        end

        def set_config(key, value)
          raise "set_config is not supported"
        end

        ####### Helper function #######

        def add_plan_to_service(svc, plan_name, plan_description)
          if @cc_api_version == "v2"
            svc["plans"] ||= {}
            svc["plans"][plan_name] = { "name" => plan_name, "description" => plan_description }
          else
            svc["plans"] ||= []
            svc["plans"] << plan_name
          end
        end

        def key_for_service(label, version, provider)
          if @cc_api_version == "v2"
            "#{label}_#{provider}"
          else
            "#{label}-#{version}"
          end
        end


      end
    end
  end
end
