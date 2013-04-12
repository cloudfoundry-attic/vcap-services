module VCAP::Services::Marketplace::Appdirect
  class NameAndProviderResolver < Struct.new(:offering_mappings)
    def resolve(appdirect_label, appdirect_provider)
      key = "#{appdirect_label}_#{appdirect_provider}".to_sym
      mapping  = offering_mappings.fetch(key)
      return mapping[:cc_name],  mapping[:cc_provider]
    end
  end
end
