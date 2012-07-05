# Copyright (c) 2009-2011 VMware, Inc.

module VCAP
  module Services
    module Atmos
      class AtmosError < VCAP::Services::Base::Error::ServiceError
        ATMOS_BACKEND_ERROR_CREATE_SUBTENAT = [31801, HTTP_INTERNAL, 'Atmos create subtenant error. Atmos error code: %s.']
        ATMOS_BACKEND_ERROR_DELETE_SUBTENAT = [31802, HTTP_INTERNAL, 'Atmos delete subtenant error. Atmos error code: %s.']
        ATMOS_BACKEND_ERROR_CREATE_USER = [31803, HTTP_INTERNAL, 'Atmos create user error. Atmos error code: %s.']
        ATMOS_BACKEND_ERROR_DELETE_USER = [31804, HTTP_INTERNAL, 'Atmos delete user error. Atmos error code: %s.']
      end
    end
  end
end
