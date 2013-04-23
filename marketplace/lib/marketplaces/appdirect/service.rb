require_relative 'plan_factory'
module VCAP
  module Services
    module Marketplace
      module Appdirect
        class Service
          INITIAL_FIELDS = %w(label provider description version info_url external_id)
          PUBLIC_API_FIELDS = %w(extra)

          attr_reader *INITIAL_FIELDS
          attr_reader *PUBLIC_API_FIELDS
          attr_reader :plans

          def self.with_extra_info(attributes, api_host, json_client=JsonHttpClient.new)
            services = attributes.collect { |attrs| new(attrs) }
            services.each do |service|
              service_details = json_client.get("#{api_host}/api/marketplace/v1/products/#{service.external_id}")
              service.assign_extra_information(service_details)
            end
          end

          def initialize(attributes)
            plans_attrs = attributes.delete('plans')
            @plans = plans_attrs.collect { |plan_attrs| PlanFactory.build(plan_attrs) }
            @extra = Yajl::Encoder.encode({})
            INITIAL_FIELDS.each do |field|
              instance_variable_set("@#{field}", attributes.fetch(field))
            end
          end

          def to_hash
            (INITIAL_FIELDS+PUBLIC_API_FIELDS).each.with_object({}) do |field, hash|
              hash[field] = public_send(field)
            end.merge('plans' => plans.collect(&:to_hash))
          end

          def assign_extra_information(extra_attributes)
            extra = {
                provider: {name: provider}
            }

            if extra_attributes.respond_to?(:fetch)
              extra.merge!(
                listing: {
                  imageUrl: extra_attributes.fetch('listing').fetch('profileImageUrl'),
                  blurb: extra_attributes.fetch('listing').fetch('blurb'),
                }
              )

              plans.each do |plan|
                plan.assign_extra_information(extra_attributes)
              end
            end
            @extra = Yajl::Encoder.encode(extra)
          end
        end
      end
    end
  end
end

