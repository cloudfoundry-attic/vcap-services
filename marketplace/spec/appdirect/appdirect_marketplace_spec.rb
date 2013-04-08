$:.unshift(File.dirname(__FILE__))
require_relative "../spec_helper"
require_relative "mocks"
require_relative "../do"
require_relative "../../lib/marketplaces/appdirect/appdirect_marketplace"
require_relative "../../lib/marketplaces/appdirect/appdirect_helper"
require_relative "../../lib/marketplaces/appdirect/appdirect_error"

require "fiber"

describe VCAP::Services::Marketplace::Appdirect::AppdirectMarketplace do

  include Do

  before :all do
    @config = load_config("appdirect")
    @logger = @config[:logger]
    @config[:appdirect][:endpoint] = Mocks.get_endpoint
    @config[:offering_whitelist] = [
      "mongolab_dev_mongolab_dev_provider",
      "mongolab_mongolab_provider",
      "asms_dev_asms_dev_provider",
      "james_dev_james_dev_provider"
    ]

    @config[:offering_mapping] = {
      :mongolab_dev_mongolab_dev_provider => {
        :cc_name => "mongolab_dev", :cc_provider => "mongolab_dev_provider", :ad_name => "mongolab_dev", :ad_provider => "mongolab_dev_provider"
      },
      :mongolab_mongolab_provider => {
        :cc_name => "mongolab", :cc_provider => "mongolab_provider", :ad_name => "mongolab", :ad_provider => "mongolab_provider"
      },
      :asms_dev_asms_dev_provider => {
        :cc_name => "asms_dev", :cc_provider => "asms_dev_provider", :ad_name => "asms_dev", :ad_provider => "asms_dev_provider"
      },
      :james_dev_james_dev_provider => {
        :cc_name => "james_dev", :cc_provider => "james_dev_provider", :ad_name => "james_dev", :ad_provider => "james_dev_provider"
      }
    }

    @config[:test_mode] = true # this way we'll use Net::Http rather than OAuthConsumer

    @appdirect = VCAP::Services::Marketplace::Appdirect::AppdirectMarketplace.new(@config)
  end

  it "get_catalog should get Activity Streams in the catalog" do
    EM.run do
      mep = nil
      Do.at(0) { mep = Mocks.create_mock_endpoint("asms_dev/") }
      Do.at(1) {
        f = Fiber.new do
          @catalog = @appdirect.get_catalog
          @catalog.should_not be_nil
          @catalog.should have(4).keys
          @catalog["asms_dev-1.0"]["description"].should == "Activity Streams Engine"
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
        fixture_file_name = "mongolab/#{VCAP::Services::Marketplace::Appdirect::AppdirectHelper::SERVICES_PATH}/post_request.json"
        fixture = JSON.parse(Mocks.load_fixture(fixture_file_name))
        provision_req = Yajl::Encoder.encode({
          :label => "#{fixture["offering"]["id"]}-#{fixture["offering"]["version"]}",
          :plan => fixture["configuration"]["plan"],
          :name => fixture["configuration"]["name"],
          :version => fixture["offering"]["version"],
          :email => fixture["user"]["email"],
          :uuid => fixture["user"]["uuid"],
          :provider => fixture["offering"]["provider"]
        })

        f = Fiber.new do
          puts "Posting: #{provision_req.inspect}"
          receipt = @appdirect.provision_service(provision_req)
          receipt.should_not be_nil
          receipt[:configuration][:name].should == fixture["configuration"]["name"]
          receipt[:service_id].should_not be_nil
          @order_id = receipt[:service_id]

          puts "Now binding the service"
          receipt = @appdirect.bind_service_instance(@order_id, {})
          receipt.should_not be_nil
          receipt[:service_id].should_not be_nil
          @binding_id = receipt[:service_id]
          receipt[:credentials].should_not be_nil

          puts "Now unbinding service - binding_id: #{@binding_id}"

          unbind_receipt = @appdirect.unbind_service(@order_id, @binding_id)
          unbind_receipt.should be_true

          puts "Now cancelling the service"
          @cancel_receipt = @appdirect.unprovision_service(@order_id)
          @cancel_receipt.should be_true
        end
        f.resume
      }
      Do.at(2) { mep.stop; EM.stop }
    end
  end
end
