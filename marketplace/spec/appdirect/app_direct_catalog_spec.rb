require_relative "../spec_helper"
require "marketplaces/appdirect/app_direct_catalog"

module VCAP::Services::Marketplace::Appdirect
  describe AppDirectCatalog do
    describe "#current_offering" do
      let(:fetcher) { double('ExtraInformationFetcher') }
      let(:client) { ->(*_) { [200, response_body] } }
      let(:filter_response) { double('filter_response').as_null_object }
      let(:filter) { double('filter', filter: filter_response) }
      let(:pass_through_filter) do
        double('filter').tap do |f|
          f.stub(:filter) { |services| services }
        end
      end
      let(:response_body) do
        JSON.dump(
          [{
            "label"=>"mongodb",
            "provider"=>"mongolab-dev",
            "version"=>"n/a",
            "description"=>"Cloud hosted and managed MongoDB",
            "plans"=>[
              {
                "id"=>"free",
                "description"=>"Free",
                "free"=>true,
                "external_id"=>"addonOffering_1"
              },
              {
                "id"=>"large",
                "description"=>"Large",
                "free"=>false,
                "external_id"=>"addonOffering_4"
              }
            ],
              "external_id"=>"7",
              "info_url"=>"https://dev3cloudfoundry.appdirect.com/apps/7"
          }]
        )
      end

      subject(:catalog) { AppDirectCatalog.new('http://example.com', client, null_object) }

      context "when appdirect returns a success response" do
        it "returns the fetched services" do
          offerings = catalog.current_offerings(pass_through_filter)

          offerings.should have(1).items
          offerings.first.label.should == "mongodb"
          offerings.first.provider.should == "mongolab-dev"
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
end
