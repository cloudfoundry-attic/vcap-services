require_relative 'plan_factory'
require_relative 'json_http_client'

module VCAP
  module Services
    module Marketplace
      module Appdirect
        class Service
          INITIAL_FIELDS = %w(label provider description version info_url external_id)
          PUBLIC_API_FIELDS = %w(extra)

          attr_reader *INITIAL_FIELDS, *PUBLIC_API_FIELDS
          attr_reader :plans

          def self.with_extra_info(attributes, api_host, json_client=JsonHttpClient.new)
            services = attributes.collect { |attrs| new(attrs) }
            services.each do |service|
              response = json_client.get("#{api_host}/api/marketplace/v1/products/#{service.external_id}")
              if response.successful?
                service.assign_extra_information(response.body)
              end
            end
          end

          def initialize(attributes)
            plans_attrs = attributes.delete('plans')
            @plans = plans_attrs.collect { |plan_attrs| PlanFactory.build(plan_attrs) }
            @extra = nil
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
              "listing" => {
                "imageUrl" => extra_attributes.fetch('listing').fetch('profileImageUrl'),
                "blurb" => extra_attributes.fetch('listing').fetch('blurb'),
              },
              "provider" => {
                "name" => extra_attributes.fetch('provider').fetch('name')
              },
            }

            plans.each do |plan|
              plan.assign_extra_information(extra_attributes)
            end

            @extra = extra
          end
        end
      end
    end
  end
end

