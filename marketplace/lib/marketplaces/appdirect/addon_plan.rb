module VCAP::Services::Marketplace::Appdirect
  class AddonPlan
    INITIAL_FIELDS = %w(id description free external_id)
    PUBLIC_API_FIELDS = %w(extra)
    attr_reader *INITIAL_FIELDS, *PUBLIC_API_FIELDS

    def initialize(attrs)
      INITIAL_FIELDS.each do |field|
        instance_variable_set("@#{field}", attrs.fetch(field))
      end
    end

    def to_hash
      (INITIAL_FIELDS + PUBLIC_API_FIELDS).inject({})do |memo, field|
        memo[field] = public_send(field); memo
      end
    end

    def addon_id
      external_id.match(/addonOffering_(\d+)/)[1].to_i
    end

    def addon_attrs(addons_attr_list)
      return addons_attr_list.find {|addon_attrs| addon_attrs.fetch('id') == addon_id }
    end

    def assign_extra_information(extra_information)
      addons_attr_list = extra_information.fetch('addonOfferings')
      addon_attrs = addon_attrs(addons_attr_list)
      payment_plan = addon_attrs.fetch('paymentPlans').first
      pricing_in_usd = payment_plan.fetch('costs').first.
        fetch('amounts').find {|cost| cost.fetch('currency') == 'USD'}

      raise ArgumentError, "A USD pricing is required" unless pricing_in_usd
      @extra = {
        'cost'    => pricing_in_usd.fetch('value').to_f,
        'bullets' => [addon_attrs.fetch('descriptionHtml')],
      }
    end
  end
end
