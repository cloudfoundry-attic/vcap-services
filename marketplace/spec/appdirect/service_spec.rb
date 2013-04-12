require_relative "../spec_helper"
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
              "external_id" => "addonOffering_1",
            },
            {
              "id" => "large",
              "description" => "Large",
              "free" => false,
              "external_id" => "addonOffering_4",
            }
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

      context 'fetching extra information' do
        let(:json_attributes) do
          {
            'listing' => {
              'profileImageUrl' => 'https://example.com/mongo-stuff.png',
              'blurb' => 'WEBSCALE!!!!11!'
            }
          }
        end

        before do
          json_client.stub(:get).and_return(json_attributes)
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
      end
    end
  end
end
