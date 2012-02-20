# Copyright (c) 2009-2011 VMware, Inc.
require "rubygems"
require "yajl"

module VCAP
  module Services
    module Base
      module Error
        class ServiceError < StandardError
          attr_reader :http_status, :error_code, :error_msg

          # HTTP status code
          HTTP_BAD_REQUEST     = 400
          HTTP_NOT_AUTHORIZED  = 401
          HTTP_FORBIDDEN       = 403
          HTTP_NOT_FOUND       = 404
          HTTP_INTERNAL        = 500
          HTTP_SERVICE_UNAVAIL = 503
          HTTP_GATEWAY_TIMEOUT = 504

          # Error Code is defined here
          #
          # e.g.
          # ERR_NAME  = [err_code, http_status,     err_message_template]
          # NOT_FOUND = [30300,    HTTP_NOT_FOUND,  '%s not found!'    ]

          # 30000 - 30099  400 Bad Request
          INVALID_CONTENT = [30000, HTTP_BAD_REQUEST, 'Invalid Content-Type']
          MALFORMATTED_REQ = [30001, HTTP_BAD_REQUEST, 'Malformatted request']
          UNKNOWN_LABEL = [30002, HTTP_BAD_REQUEST, 'Unknown label']
          UNKNOWN_PLAN = [30003, HTTP_BAD_REQUEST, 'Unknown plan %s']

          # 30100 - 30199  401 Unauthorized
          NOT_AUTHORIZED = [30100, HTTP_NOT_AUTHORIZED, 'Not authorized']

          # 30200 - 30299  403 Forbidden

          # 30300 - 30399  404 Not Found
          NOT_FOUND = [30300, HTTP_NOT_FOUND, '%s not found']

          # 30500 - 30599  500 Internal Error
          INTERNAL_ERROR = [30500, HTTP_INTERNAL, 'Internal Error']

          # 30600 - 30699  503 Service Unavailable
          SERVICE_UNAVAILABLE = [30600, HTTP_SERVICE_UNAVAIL, 'Service unavailable']

          # 30700 - 30799  500 Gateway Timeout
          GATEWAY_TIMEOUT = [30700, HTTP_GATEWAY_TIMEOUT, 'Gateway timeout']

          # 31000 - 32000  Service-specific Error
          # Defined in services directory, for example mongodb/lib/mongodb_service/

          def initialize(code, *args)
            @http_status = code[1]
            @error_code  = code[0]
            @error_msg   = sprintf(code[2], *args)
          end

          def to_s
            "Error Code: #{@error_code}, Error Message: #{@error_msg}"
          end

          def to_hash
            {
              'status' => @http_status,
              'msg' => {
                'code' => @error_code,
                'description' => @error_msg
              }
            }
          end
        end


        def success(response = true)
          {'success' => true, 'response' => response}
        end

        def failure(exception)
          {'success' => false, 'response' => exception.to_hash}
        end

        def internal_fail()
          e = ServiceError.new(ServiceError::INTERNAL_ERROR)
          failure(e)
        end

        def timeout_fail()
          e = ServiceError.new(ServiceError::GATEWAY_TIMEOUT)
          failure(e)
        end

        def parse_msg(msg)
          Yajl::Parser.parse(msg)
        end

      end
    end
  end
end
