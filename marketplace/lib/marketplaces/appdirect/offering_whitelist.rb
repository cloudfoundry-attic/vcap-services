module VCAP
  module Services
    module Marketplace
      module Appdirect
        class OfferingWhitelist
          attr_reader :whitelist, :logger

          def initialize(whitelist, logger)
            @whitelist = whitelist || []
            @logger = logger
          end

          def filter(services)
            catalog = []
            services.each do |service|
              if include?(service)
                logger.info("Accepting whitelisted service: #{label(service)} from provider: #{provider(service)}")
                catalog << service
              else
                logger.warn("Ignoring service Offering:  #{label(service)} from provider: #{provider(service)} since it is not whitelisted")
              end
            end
            catalog
          end

          def include?(service)
            whitelist.include?("#{label(service)}_#{provider(service)}")
          end

          private

          def provider(service)
            service.fetch("provider")
          end

          def label(service)
            service.fetch("label")
          end
        end
      end
    end
  end
end
