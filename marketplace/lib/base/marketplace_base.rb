# Copyright (c) 2009-2012 VMware, Inc.

require 'abstract'

module VCAP
  module Services
    module Marketplace
      class Base

        def initialize(opts_unused)
        end

        # name
        abstract :name

        # get_catalog
        abstract :get_catalog

        # offering_disabled?(name, offerings)
        abstract :offering_disabled?

        # provision_service(request_body)
        abstract :provision_service

        # unprovision_service(service_id)
        abstract :unprovision_service

        # bind_service_instance(service_id, bind_options)
        abstract :bind_service_instance

        # unbind_service(service_id, binding_id)
        abstract :unbind_service

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
