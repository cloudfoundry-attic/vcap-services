module VCAP
  module Services
    module Marketplace
      module Appdirect
        class JsonHttpClient
          class Response
            SUCCESS_STATUS_RANGE = 200..299

            attr_reader :body, :raw_body, :status
            def initialize(status, raw_body)
              @status = status
              @raw_body = raw_body

              if successful?
                @body = Yajl::Parser.parse(raw_body)
              end
            end

            def successful?
              SUCCESS_STATUS_RANGE.cover? status
            end
          end

          def logger
            @logger ||= VCAP::Logging.logger(File.basename($0))
          end

          def get(url)
            f = Fiber.current
            http = EM::HttpRequest.new(url).get
            http.callback { f.resume }
            http.errback { f.resume }
            Fiber.yield

            response = Response.new(http.response_header.status, http.response)
            log_response(url, response)

            response
          end

          private

          def log_response(url, response)
            if response.successful?
              logger.debug("JsonHttpClient#get(#{url.inspect}) succeeded with status #{response.status.inspect}, body #{response.raw_body[0..50]} (truncated at 50chars)")
            else
              logger.warn("JsonHttpClient#get(#{url.inspect}) failed with status #{response.status.inspect}, body #{response.raw_body.inspect}")
            end
          end

        end
      end
    end
  end
end
