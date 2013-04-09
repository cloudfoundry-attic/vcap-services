require 'spec_helper'
require 'marketplaces/appdirect/appdirect_helper'

module VCAP::Services::Marketplace::Appdirect
  describe AppdirectHelper do
    describe "#app_direct_catalog" do
      let(:required_options) { {key: '', secret: ''} }
      let(:endpoint) { 'http://example.com/endpoint' }
      let(:logger) { double('logger').as_null_object }
      let(:http_client_matcher) { respond_to(:call).with(4).arguments }

      it 'is set on initialization' do
        catalog = double('appdirect catalog')
        AppDirectCatalog.should_receive(:new).with(endpoint, http_client_matcher, logger).
          and_return(catalog)
        helper = AppdirectHelper.new({appdirect: {endpoint: endpoint}.merge(required_options)}, logger)
        helper.app_direct_catalog.should == catalog
      end
    end

    describe "#load_catalog" do
      let(:helper) { AppdirectHelper.new({appdirect: {endpoint: '', key: '', secret: ''}}, null_object) }

      it "returns the catalog's current offerings" do
        current_offerings = double('current_offerings')
        helper.app_direct_catalog.should_receive(:current_offerings).with(helper.offering_whitelist).
          and_return(current_offerings)
        helper.load_catalog.should == current_offerings
      end
    end
  end
end

