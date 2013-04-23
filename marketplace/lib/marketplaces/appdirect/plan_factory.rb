require_relative 'edition_plan'
require_relative 'addon_plan'
module VCAP
  module Services
    module Marketplace
      module Appdirect
        class PlanFactory
          def self.build(attrs)
            type = attrs['external_id'].split('_').first
            case type
            when 'addonOffering'
              AddonPlan.new(attrs)
            when 'edition'
              EditionPlan.new(attrs)
            else
              raise ArgumentError.new("Unknown type #{type}")
            end
          end
        end
      end
    end
  end
end


