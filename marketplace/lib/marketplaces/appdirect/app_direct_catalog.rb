require_relative 'appdirect_error'
require_relative 'service'

module VCAP
  module Services
    module Marketplace
      module Appdirect

        class AppDirectCatalog < Struct.new(:api_host, :client, :logger)
          OFFERINGS_PATH = "api/custom/cloudfoundry/v1/offerings"

          def current_offerings(filter)
            url = "#{api_host}/#{OFFERINGS_PATH}"
            logger.debug("Getting service listing from: #{url}")
            http_status, response_body = client.call("get", url, nil, nil)

            if http_status == 200
              service_attributes = JSON.parse(response_body)
              service_attributes = filter.filter(service_attributes)
              Service.with_extra_info(service_attributes, api_host)
            else
              logger.error("Failed to get catalog #{http_status}")
              raise AppdirectError.new(AppdirectError::APPDIRECT_ERROR_GET_LISTING, http_status)
            end
          end
        end
      end
    end
  end
end
