module VCAP
  module Services
    module Marketplace
      module Appdirect
        class JsonHttpClient
          SUCCESS_STATUS_RANGE = 200..299
          def get(url)
            f = Fiber.current
            http = EM::HttpRequest.new(url).get
            http.callback { f.resume }
            http.errback { f.resume }
            Fiber.yield

            if SUCCESS_STATUS_RANGE.cover? http.response_header.status
              return Yajl::Parser.parse(http.response)
            end
          end
        end
      end
    end
  end
end
