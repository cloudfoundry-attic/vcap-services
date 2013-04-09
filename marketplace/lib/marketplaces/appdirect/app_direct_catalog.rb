require_relative 'appdirect_error'
module VCAP
  module Services
    module Marketplace
      module Appdirect
        class Service
          FIELDS = %w(label provider description plans version info_url)

          attr_reader *FIELDS

          def initialize(attributes)
            FIELDS.each do |field|
              instance_variable_set("@#{field}", attributes.fetch(field))
            end
          end

          def to_hash
            FIELDS.each.with_object({}) do |field, hash|
              hash[field] = public_send(field)
            end
          end
        end

        class AppDirectCatalog < Struct.new(:endpoint, :client, :logger)
          OFFERINGS_PATH = "api/custom/cloudfoundry/v1/offerings"

          def current_offerings(filter)
            url = "#{endpoint}/#{OFFERINGS_PATH}"
            logger.debug("Getting service listing from: #{url}")
            http_status, response_body = client.call("get", url, nil, nil)

            if http_status == 200
              service_attributes = JSON.parse(response_body)
              service_attributes = filter.filter(service_attributes)
              logger.info("Got #{service_attributes.size} services from AppDirect")
              service_attributes.collect {|attributes| Service.new(attributes) }
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
