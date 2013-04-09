require "spec_helper"
require "marketplaces/appdirect/app_direct_catalog"

module VCAP::Services::Marketplace::Appdirect
  describe AppDirectCatalog do
    describe "#current_offering" do
      let(:fetcher) { double('ExtraInformationFetcher') }
      let(:client) { ->(*_) { [200, '{"response": "body"}'] } }
      let(:filter_response) { double('filter_response').as_null_object }
      let(:filter) { double('filter', filter: filter_response) }

      subject(:catalog) { AppDirectCatalog.new('http://example.com', client, null_object) }

      it "creates an ExtraInformationFetcher" do
        ExtraInformationFetcher.should_receive(:new).with(filter_response).and_return(null_object)

        catalog.current_offerings(filter)
      end

      it "integrates extra data" do
        ExtraInformationFetcher.stub(:new).and_return(fetcher)

        offerings_with_extra_info = double('offerings with extra info')
        fetcher.should_receive(:fetch_extra_information).and_return(offerings_with_extra_info)

        catalog.current_offerings(filter).should == offerings_with_extra_info
      end
    end
  end
end
