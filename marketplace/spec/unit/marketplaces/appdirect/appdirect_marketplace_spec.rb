require_relative "../../../spec_helper"
require "marketplaces/appdirect/appdirect_marketplace"

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
