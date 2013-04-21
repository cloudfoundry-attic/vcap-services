require_relative "../../../spec_helper"
require "marketplaces/appdirect/service"

module VCAP::Services::Marketplace::Appdirect
  describe Service do
    describe ".with_extra_info" do
      let(:service_external_id) { "7" }

      let(:all_attributes) do
        [attributes]
      end

      let(:attributes) do
        {
          "label" => "mongodb",
          "provider" => "mongolab",
          "version" => "n/a",
          "description" => "Cloud hosted and managed MongoDB",
          "external_id" => service_external_id,
          "info_url" => "https://dev3cloudfoundry.appdirect.com/apps/7",
          "plans" => [
            {
              "id" => "free",
              "description" => "Free",
              "free" => true,
              "external_id" => "addonOffering_98",
            },
          ],
        }
      end

      let(:api_host){ 'http://example.com' }
      let(:json_client) { double('http client') }

      describe "#external_id" do
        it "returns correctly" do
          service = Service.new(attributes)
          service.external_id.should == service_external_id
        end
      end

      let(:public_api_service_attributes) do
        {
          'listing' => {
            'profileImageUrl' => 'https://example.com/mongo-stuff.png',
            'blurb' => 'WEBSCALE!!!!11!'
          },
          "addonOfferings" => [
            {
              "id" => 98,
              "name" => "Free",
              "code" => "mongodb => mongolab:free",
              "description" => "Free",
              "descriptionHtml" => "Free",
              "status" => "ACTIVE",
              "stacked" => true,
              "paymentPlans" => [
                {
                  "id" => 190,
                  "frequency" => "MONTHLY",
                  "contract" => {
                    "blockSwitchToShorterContract" => false,
                    "blockContractDowngrades" => false,
                    "blockContractUpgrades" => false,
                  },
                  "allowCustomUsage" => false,
                  "keepBillDateOnUsageChange" => false,
                  "separatePrepaid" => false,
                  "costs" => [
                    {
                      "unit" => "NOT_APPLICABLE",
                      "minUnits" => 0E-10,
                      "maxUnits" => nil,
                      "meteredUsage" => false,
                      "increment" => nil,
                      "pricePerIncrement" => false,
                      "amounts" => [
                        {
                          "currency" => "USD",
                          "value" => 20
                        }
                      ]
                    }
                  ]
                }
              ]
            }
          ]
        }
      end

      context 'fetching service extra information' do
        before do
          json_client.stub(:get).and_return(public_api_service_attributes)
        end

        describe "#to_hash" do
          it "has right keys" do
            service = Service.with_extra_info(all_attributes, api_host, json_client).first
            service.to_hash.keys.should =~ %w[label provider extra description external_id version info_url plans]
          end

          it "expresses plans as an array of hashes" do
            service = Service.with_extra_info(all_attributes, api_host, json_client).first
            plan = service.to_hash.fetch('plans').first
            plan.should be_a Hash
          end
        end

        it "returns Services with the given attributes" do
          services = Service.with_extra_info(all_attributes, api_host, json_client)
          services.should have(1).items
        end

        it "gets extra information from the AppDirect catalog API" do
          json_client.should_receive(:get).with("#{api_host}/api/marketplace/v1/products/#{service_external_id}")
          Service.with_extra_info(all_attributes, api_host, json_client)
        end

        it "merges the extra information to the service attributes" do
          services = Service.with_extra_info(all_attributes, api_host, json_client)
          extra = Yajl::Parser.parse(services.first.extra)

          extra.fetch('provider').fetch('name').should == 'mongolab'
          extra.fetch('listing').fetch('imageUrl').should == 'https://example.com/mongo-stuff.png'
          extra.fetch('listing').fetch('blurb').should == 'WEBSCALE!!!!11!'
        end

        it "returns the basic info only when extra information cannot be found" do
          json_client.stub(:get).and_return(404)
          services = Service.with_extra_info(all_attributes, api_host, json_client)
          extra = Yajl::Parser.parse(services.first.extra)

          extra.fetch('provider').fetch('name').should == 'mongolab'
          extra['listing'].should be_nil
        end
      end

      context 'fetching addon plans extra information' do

        before do
          json_client.stub(:get).and_return(public_api_service_attributes)
        end

        it 'populates "extra" correctly' do
          service = Service.with_extra_info(all_attributes, api_host, json_client).first
          extra = service.plans.first.extra
          extra.fetch('cost').should == 20.00
        end
      end
    end
  end
end
