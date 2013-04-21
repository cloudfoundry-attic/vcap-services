module VCAP
  module Services
    module Marketplace
      module Appdirect
        class JsonHttpClient
          SUCCESS_STATUS_RANGE = 200..299

          def logger
            @logger ||= VCAP::Logging.logger(File.basename($0))
          end

          def get(url)
            f = Fiber.current
            http = EM::HttpRequest.new(url).get
            http.callback { f.resume }
            http.errback { f.resume }
            Fiber.yield

            if SUCCESS_STATUS_RANGE.cover? http.response_header.status
              logger.debug("JsonHttpClient#get(#{url.inspect}) succeeded with status #{http.response_header.status.inspect}, body #{http.response[0..50]} (truncated at 50chars)")
              Yajl::Parser.parse(http.response)
            else
              logger.warn("JsonHttpClient#get(#{url.inspect}) failed with status #{http.response_header.status.inspect}, body #{http.response.inspect}")
              http.response_header.status   # returns http status on failure
            end
          end
        end
      end
    end
  end
end
