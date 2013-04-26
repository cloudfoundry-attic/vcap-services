module VCAP::Services::Marketplace::Appdirect
  class EditionPlan
    def initialize(attrs)
      whitelist = %w(id description free external_id)
      @attributes = {}
      whitelist.each do |field|
        value = attrs.fetch(field)
        raise ArgumentError, "Missing value #{field.inspect}" if value.nil?
        @attributes[field] = value
      end
      @attributes['extra'] = nil
    end

    def extra
      @attributes['extra']
    end

    def to_hash
      @attributes
    end

    def edition_attrs(edition_attr_list)
      edition_attr_list.find {|edition_attrs| edition_attrs.fetch("id") == edition_id }
    end

    def assign_extra_information(extra_information)
      edition_attr_list = extra_information.fetch("pricing").fetch('editions')
      edition_attrs = self.edition_attrs(edition_attr_list)
      payment_plan = edition_attrs.fetch('plans').first

      pricing_in_usd = payment_plan.fetch('costs').first.
        fetch('amounts').find {|cost| cost.fetch('currency') == 'USD'}
      raise ArgumentError, "A USD pricing is required" unless pricing_in_usd

      attributes['extra'] = {
        'cost'    => pricing_in_usd.fetch('value').to_f,
        'bullets' => edition_attrs.fetch('bullets'),
      }
    end

    private
    def edition_id
      @attributes.fetch("external_id").match(/edition_(\d+)/)[1].to_i
    end

    attr_reader :attributes
  end
end
