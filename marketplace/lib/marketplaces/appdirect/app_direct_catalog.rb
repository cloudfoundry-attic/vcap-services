require_relative 'appdirect_error'
require_relative 'extra_information_fetcher'
module VCAP
  module Services
    module Marketplace
      module Appdirect
        class AppDirectCatalog < Struct.new(:endpoint, :client, :logger)
          OFFERINGS_PATH = "api/custom/cloudfoundry/v1/offerings"

          def current_offerings(filter)
            url = "#{endpoint}/#{OFFERINGS_PATH}"
            logger.debug("Getting service listing from: #{url}")
            http_status, response_body = client.call("get", url, nil, nil)

            if http_status == 200
              services = JSON.parse(response_body)
              catalog  = filter.filter(services)
              logger.info("Got #{catalog.size} services from AppDirect")
              # ExtraInformationFetcher.new(catalog).fetch_extra_information
              catalog
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
