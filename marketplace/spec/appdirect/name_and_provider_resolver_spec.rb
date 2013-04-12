require_relative "../spec_helper"
require "marketplaces/appdirect/name_and_provider_resolver"

describe VCAP::Services::Marketplace::Appdirect::NameAndProviderResolver do
  subject do
    described_class.new(offering_mappings)
  end
  let(:offering_mappings) do
    {
      :fooservice_foolab => {
        :cc_name => "foo",
        :cc_provider => "foo-provider",
      }
    }
  end
  it "looks up the (name,provider) pair that is advertised to CC" do
    subject.resolve('fooservice', 'foolab').should == ['foo', 'foo-provider']
  end

  it "raises exception when the (name, provider) pair is unknown" do
    expect{ subject.resolve('nohere', 'orthere') }.to raise_error(KeyError)
  end
end
