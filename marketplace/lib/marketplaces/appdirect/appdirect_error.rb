# Copyright (c) 2009-2011 VMware, Inc.

module VCAP
  module Services
    module Marketplace
      module Appdirect
        class AppdirectError < VCAP::Services::Base::Error::ServiceError
          APPDIRECT_ERROR_GET_LISTING = [33101, HTTP_INTERNAL, 'AppDirect get_listing error. AppDirect error code: %s.']
          APPDIRECT_ERROR_PURCHASE    = [33102, HTTP_INTERNAL, 'AppDirect purchase_service error. AppDirect error code: %s.']
          APPDIRECT_ERROR_CANCEL      = [33103, HTTP_INTERNAL, 'AppDirect cancel_service error. AppDirect error code: %s.']
          APPDIRECT_ERROR_BIND        = [33104, HTTP_INTERNAL, 'AppDirect bind_service error. AppDirect error code: %s.']
          APPDIRECT_ERROR_UNBIND      = [33105, HTTP_INTERNAL, 'AppDirect unbind_service error. AppDirect error code: %s.']
        end
      end
    end
  end
end
