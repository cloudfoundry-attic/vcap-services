require_relative "../spec_helper"
require "marketplaces/appdirect/name_and_provider_resolver"

describe VCAP::Services::Marketplace::Appdirect::NameAndProviderResolver do
  let(:name_and_provider_resolver) do
    described_class.new(offering_mappings)
  end

  let(:offering_mappings) do
    {
      :"foo-ad_foo-provider-ad" => {
        cc_name: "foo-cc",
        cc_provider: "foo-provider-cc",
        ad_name: "foo-ad",
        ad_provider: "foo-provider-ad",
      }
    }
  end

  describe "#resolve_from_appdirect_to_cc" do
    context "when the (name, provider) pair is advertised to CC" do
      it "returns the correct mapping" do
        name_and_provider_resolver.resolve_from_appdirect_to_cc('foo-ad', 'foo-provider-ad').should == ['foo-cc', 'foo-provider-cc']
      end
    end

    context "when the (name, provider) pair is unknown" do
      it "raises an exception" do
        expect do
          name_and_provider_resolver.resolve_from_appdirect_to_cc('nohere', 'orthere')
        end.to raise_error(KeyError)
      end
    end
  end

  describe "#resolve_from_cc_to_appdirect" do
    context "when the (name, provider) pair is advertised by Appdirect" do
      it "returns the correct mapping" do
        name_and_provider_resolver.resolve_from_cc_to_appdirect('foo-cc', 'foo-provider-cc').should == ['foo-ad', 'foo-provider-ad']
      end
    end

    context "when the (name, provider) pair is unknown" do
      it "raises an exception" do
        expect do
          name_and_provider_resolver.resolve_from_cc_to_appdirect('nohere', 'orthere')
        end.to raise_error(ArgumentError)
      end
    end
  end
end
