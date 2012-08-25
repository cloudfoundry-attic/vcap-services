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

        def generate_cc_advertise_request(name, bsvc, active = true)
          {}
        end

        def provision_service(request_body)
        end

        def unprovision_service(service_id)
        end

        def bind_service_instance(service_id, request)
        end

        def unbind_service(service_id, binding_id)
        end

      end
    end
  end
end
