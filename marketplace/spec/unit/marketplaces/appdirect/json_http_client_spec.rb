require_relative '../../../spec_helper'
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

      it "logs failure" do
        stub_request(:any, url).to_return(body: 'Go home!', status: 401)
        fake_logger = double
        subject.instance_variable_set(:@logger, fake_logger)

        fake_logger.should_receive(:warn).with(/#{url}.*401.*Go home!/)

        EventMachine.run_block do
          Fiber.new do
            json_http_client.get(url)
          end.resume
        end

      end
    end
  end
end
