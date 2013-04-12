require_relative '../spec_helper'
require 'marketplaces/appdirect/json_http_client'

module VCAP::Services::Marketplace::Appdirect
  describe JsonHttpClient do
    let(:parsed_json) { {'key' => 'value'} }
    let(:raw_body) { Yajl::Encoder.encode(parsed_json) }
    let(:url) { 'http://example.com' }

    before do
      stub_request(:any, url).to_return(body: raw_body)
    end

    subject(:json_http_client) {
      JsonHttpClient.new
    }

    describe "#get" do
      it "hits the url" do
        EventMachine.run_block do
          Fiber.new do
            json_http_client.get(url)
          end.resume
        end
        a_request(:get, url).should have_been_made
      end

      it "parses the json" do
        result = nil
        EventMachine.run_block do
          Fiber.new do
            result = json_http_client.get(url)
          end.resume
        end
        result.should == parsed_json
      end

      it "propagates the error when connection fails"
    end
  end
end
