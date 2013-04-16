module VCAP::Services::Marketplace::Appdirect
  NameAndProviderResolver = Struct.new(:offering_mappings) do
    def resolve_from_appdirect_to_cc(ad_label, ad_provider)
      mapping  = offering_mappings.fetch(build_key(ad_label, ad_provider))
      return mapping[:cc_name],  mapping[:cc_provider]
    end

    def resolve_from_cc_to_appdirect(cc_label, cc_provider)
      mapping = offering_mappings.values.detect {|v| v[:cc_name] == cc_label && v[:cc_provider] == cc_provider }
      raise ArgumentError, "Requested mapping for unknown label: #{cc_label} / provider: #{cc_provider}" unless mapping
      return mapping[:ad_name],  mapping[:ad_provider]
    end

    private

    def build_key(label, provider)
      "#{label}_#{provider}".to_sym
    end
  end
end
