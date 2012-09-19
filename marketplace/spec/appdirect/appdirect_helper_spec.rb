$:.unshift(File.dirname(__FILE__))
require_relative "../spec_helper"
require_relative "mocks"
require_relative "../do"
require_relative "../../lib/marketplaces/appdirect/appdirect_helper"
require_relative "../../lib/marketplaces/appdirect/appdirect_error"

require "fiber"

describe VCAP::Services::Marketplace::Appdirect::AppdirectHelper do

  include Do

  before :all do
    @config = load_appdirect_config
    @logger = @config[:logger]
    @config[:appdirect][:endpoint] = Mocks.get_endpoint
    @config[:offering_whitelist] = ["mongolab_dev", "mongolab", "asms_dev", "james_dev"]

    @appdirect = VCAP::Services::Marketplace::Appdirect::AppdirectHelper.new(@config, @logger)
  end

  it "get_catalog should get Activity Streams in the catalog" do
    EM.run do
      mep = nil
      Do.at(0) { mep = Mocks.create_mock_endpoint("asms_dev/") }
      Do.at(1) {
        f = Fiber.new do
          @catalog = @appdirect.get_catalog
          @catalog.should_not be_nil
          @catalog.keys.count.should == 4
          @catalog["asms_dev-1.0"]["name"].should == "Activity Streams"
        end
        f.resume
      }
      Do.at(2) { mep.stop; EM.stop }
    end
  end

  it "should be able to purchase, bind, unbind and cancel service" do
    EM.run do
      mep = nil
      Do.at(0) { mep = Mocks.create_mock_endpoint("mongolab/") }
      Do.at(1) {
        req = load_fixture("mongolab/#{VCAP::Services::Marketplace::Appdirect::AppdirectHelper::SERVICES_PATH}/post_request.json")
        f = Fiber.new do
          puts "Posting#{req.inspect}"
          receipt = @appdirect.purchase_service(req)
          receipt.should_not be_nil
          receipt["offering"]["id"].should ==  "mongolab_dev"
          receipt["uuid"].should_not be_nil
          @order_id = receipt["uuid"]
          receipt["id"].should_not be_nil

          puts "Now binding the service"

          req = {}
          receipt = @appdirect.bind_service(req, @order_id)
          puts "Got resp #{receipt.inspect}"
          receipt.should_not be_nil
          receipt["uuid"].should_not be_nil
          @binding_id = receipt["uuid"]
          receipt["credentials"].should_not be_nil

          puts "Now unbinding service - binding_id: #{@binding_id}"

          unbind_receipt = @appdirect.unbind_service(@order_id, @binding_id)
          unbind_receipt.should be_true

          puts "Now cancelling the service"
          @cancel_receipt = @appdirect.cancel_service(@order_id)
          @cancel_receipt.should be_true
        end
        f.resume
      }
      Do.at(2) { mep.stop; EM.stop }
    end
  end
end
