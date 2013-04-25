require_relative '../../../spec_helper'
require 'marketplaces/appdirect/service'

module VCAP::Services::Marketplace::Appdirect
  describe Service do
    describe ".with_extra_info" do
      context "when one of the services does not have a product page" do
        let(:api_host) { 'http://example.com' }
        let(:service_external_id) { 47 }

        before do
          stub_request(:get, "#{api_host}/api/marketplace/v1/products/#{service_external_id}").
            to_return(status: 404)
        end

        it "leaves the service and plan extra fields blank" do
          EM.run do
            Fiber.new do
              services = described_class.with_extra_info([
                {
                  "label"       => 'mongolab-dev',
                  "provider"    => 'objectlabs',
                  "description" => '',
                  "info_url"    => '',
                  "version"     => '1.0',
                  "external_id" =>  "47",
                  "plans"       => [
                    {
                      "external_id" => "addonOffering_123",
                      "id"          => "mega-platinum",
                      "description" => "very high touch",
                      "free"        => false,
                    },
                    {
                      "external_id" => "edition_456",
                      "id"          => "gold-plated-latinum",
                      "description" => "even nicer",
                      "free"        => false,
                    }
                  ],
                }
              ], api_host)

              service = services.fetch(0)
              service.extra.should be_nil
              service.plans.should have(2).entries
              service.plans.each do |plan|
                plan.extra.should be_nil
              end

              EM.stop
            end.resume
          end
        end
      end
    end
  end
end
