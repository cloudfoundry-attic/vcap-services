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
              return Yajl::Parser.parse(http.response)
            else
              logger.warn("JsonHttpClient#get(#{url.inspect}) failed with status #{http.response_header.status.inspect}, body #{http.response.inspect}")
            end
          end
        end
      end
    end
  end
end
