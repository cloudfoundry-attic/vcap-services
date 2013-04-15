require_relative "../spec_helper"
require_relative "mocks"
require_relative "../do"
require_relative "../../lib/marketplaces/appdirect/appdirect_marketplace"
require_relative "../../lib/marketplaces/appdirect/appdirect_helper"
require_relative "../../lib/marketplaces/appdirect/appdirect_error"

describe VCAP::Services::Marketplace::Appdirect::AppdirectMarketplace do

  let(:mock_helper) { double("helper", load_catalog: services) }
  let(:services) { [asms_service] }

  let(:asms_service) {
    VCAP::Services::Marketplace::Appdirect::Service.new(
     'description' => "Activity Streams Engine",
     'external_id' => "asms_dev",
     'label'       => 'label',
     'provider'    => 'asms_provider',
     'plans'       => [],
     'version'     => '2.0',
     'info_url'    => 'http://example.com/asms_dev'
    )
   }

  before do
    VCAP::Services::Marketplace::Appdirect::AppdirectHelper.stub(new: mock_helper)
    VCAP::Services::Marketplace::Appdirect::NameAndProviderResolver.stub(new: name_and_provider_resolver)
  end

  let(:ad_provider) { 'asms_provider' }
  let(:ad_label)    { 'asms' }
  let(:name_and_provider_resolver) do
    double('resolver',
           resolve_from_appdirect_to_cc: ['asms', 'asms_provider'],
           resolve_from_cc_to_appdirect: [ad_label, ad_provider]
          )
  end

  subject(:appdirect_marketplace) do
    described_class.new(
      appdirect: {
        endpoint: 'endpoint',
        key: 'k',
        secret: 's',
      },
      logger: null_object
    )
  end

  it "creates a AppdirectHelper with options" do
    VCAP::Services::Marketplace::Appdirect::AppdirectHelper.should_receive(:new).with(kind_of(Hash), anything)
    appdirect_marketplace
  end

  it "creates a NameAndProviderResolver" do
    VCAP::Services::Marketplace::Appdirect::NameAndProviderResolver.should_receive(:new).with(kind_of(Hash))
    appdirect_marketplace
  end

  describe "#get_catalog" do
    it "does something useful" do
      catalog = appdirect_marketplace.get_catalog
      catalog.should_not be_nil
      catalog.should have(1).keys

      asms_service = catalog["asms-2.0"]
      asms_service["id"].should == "asms"
      asms_service["version"].should == "2.0"
      asms_service["description"].should == "Activity Streams Engine"
      asms_service["info_url"].should == "http://example.com/asms_dev"
      asms_service["plans"].should be_empty
      asms_service["provider"].should == "asms_provider"
    end
  end

  describe "#provision_service" do
    let(:request) do
      VCAP::Services::Api::GatewayProvisionRequest.new(
        label: 'mongo-dev',
        name: 'mongo name',
        plan: 'free',
        email: '',
        version: '',
        space_guid: 'space-guid',
        organization_guid: 'organization-guid'
      )
    end
    let(:request_body) { request.encode }

    it "sends correct messages to the helper" do
      mock_helper.should_receive(:purchase_service).
        with do |opts|
          opts['space']['uuid'].should == request.space_guid
          opts['space']['organization']['uuid'].should == request.organization_guid
          opts['space']['email'].should ==  "#{request.space_guid}@cloudfoundry.com"
          opts['offering']['label'].should ==  ad_label
          opts['offering']['provider'].should ==  ad_provider
          opts['configuration']['plan']['id'].should == request.plan
          opts['configuration']['name'].should == request.name
        end.
        and_return(
          'credentials' => {},
          'id'          => 'receipt_id'
        )
      appdirect_marketplace.provision_service(request_body)
    end

  end
end

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

    @appdirect = VCAP::Services::Marketplace::Appdirect::AppdirectMarketplace.new(@config.merge(logger: Logger.new('/dev/null')))
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
          receipt = @appdirect.provision_service(provision_req)
          receipt.should_not be_nil
          receipt[:configuration][:name].should == fixture["configuration"]["name"]
          receipt[:service_id].should_not be_nil
          @order_id = receipt[:service_id]

          receipt = @appdirect.bind_service_instance(@order_id, {})
          receipt.should_not be_nil
          receipt[:service_id].should_not be_nil
          @binding_id = receipt[:service_id]
          receipt[:credentials].should_not be_nil


          unbind_receipt = @appdirect.unbind_service(@order_id, @binding_id)
          unbind_receipt.should be_true

          @cancel_receipt = @appdirect.unprovision_service(@order_id)
          @cancel_receipt.should be_true
        end
        f.resume
      }
      Do.at(2) { mep.stop; EM.stop }
    end
  end
end
