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

      context "when the response status is 2XX" do
        it "parses the json" do
          response = nil
          EventMachine.run_block do
            Fiber.new do
              response = json_http_client.get(url)
            end.resume
          end
          response.body.should == parsed_json
        end
      end

      context "when the response status is not 2XX" do
        before do
          stub_request(:any, url).to_return(body: 'Go home!', status: 401)
        end

        it "returns a failed response" do
          response = nil
          EventMachine.run_block do
            Fiber.new do
              response = json_http_client.get(url)
            end.resume
          end

          response.should_not be_successful
        end

        it "exposes the status code" do
          response = nil
          EventMachine.run_block do
            Fiber.new do
              response = json_http_client.get(url)
            end.resume
          end

          response.status.should == 401
        end

        it "logs failure" do
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
end
