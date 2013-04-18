require_relative "../../../spec_helper"
require "marketplaces/appdirect/app_direct_catalog"

module VCAP::Services::Marketplace::Appdirect
  describe AppDirectCatalog do
    describe "#current_offering" do
      let(:client) { ->(*_) { [200, response_body] } }
      let(:filter_response) { double('filter_response').as_null_object }
      let(:filter) { double('filter', filter: filter_response) }
      let(:api_host){ 'http://example.com' }
      let(:response_body) { '[{}]' }

      before do
        Service.stub(:with_extra_info).and_return(null_object)
      end

      subject(:catalog) { AppDirectCatalog.new(api_host, client, null_object) }
      it "uses the client to fetch information" do
        client.should_receive(:call).
          with('get', 'http://example.com/api/custom/cloudfoundry/v1/offerings', nil, nil).
          and_return([200, response_body])
        catalog.current_offerings(filter)
      end

      it "filters the returned JSON" do
        filter.should_receive(:filter).with(JSON.parse(response_body))
        catalog.current_offerings(filter)
      end

      it "passes the filtered results to Service to have more information" do
        services_with_extra_info = double('services with extra info')
        Service.should_receive(:with_extra_info).with(filter_response, api_host).and_return(services_with_extra_info)
        catalog.current_offerings(filter).should == services_with_extra_info
      end
    end
  end
end
