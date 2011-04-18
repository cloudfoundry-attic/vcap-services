# Copyright (c) 2009-2011 VMware, Inc.
module VCAP
  module Services
    module Base
      module Error
        class ServiceError < StandardError
          attr_reader :http_status

          HTTP_BAD_REQUEST    = 400
          HTTP_NOT_AUTHORIZED = 401
          HTTP_FORBIDDEN      = 403
          HTTP_NOT_FOUND      = 404
          HTTP_INTERNAL       = 500
          HTTP_BAD_GATEWAY    = 502

          # ERR_NAME  = [err_code, http_status,     err_message_template]
          # NOT_FOUND = [30300,    HTTP_NOT_FOUND,  '%s not found!'    ]

          # 30000 - 30099  GW Error

          # 30100 - 30199  401 Unauthorized
          NOT_AUTHORIZED = [30100, HTTP_NOT_AUTHORIZED, 'not authorized!']

          # 30200 - 30299  403 Forbidden

          # 30300 - 30399  404 Not Found
          NOT_FOUND = [30300, HTTP_NOT_FOUND, '%s not found!']

          # 30500 - 30599  500 Internal Error
          INTERNAL_ERROR = [30500, HTTP_INTERNAL, 'Internal Error!']

          # 31000 - 32000  Service-specific Error
          # Defined in services directory, for example mongodb/lib/mongodb_service/
          TEST_ERROR = [31000, HTTP_INTERNAL, '%s, %s, all down!']

          def initialize(code, *args)
            @http_status = code[1]
            @error_code  = code[0]
            @error_msg   = sprintf(code[2], *args)
          end

          def to_s
            "Error Code: #{@error_code}, Error Message: #{@error_msg}"
          end

          def to_json
            Yajl::Encoder.encode([@http_status, {:code => @error_code, :description => @error_msg}])
          end
        end
      end
    end
  end
end
