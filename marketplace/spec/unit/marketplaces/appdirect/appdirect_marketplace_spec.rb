require_relative "../../../spec_helper"
require "marketplaces/appdirect/appdirect_marketplace"

module VCAP::Services::Marketplace::Appdirect
  describe AppdirectMarketplace do
    let(:mock_helper) { double("helper", load_catalog: services) }
    let(:services) { [ stub('Service', to_hash: service_hash) ] }
    let(:service_hash) {
      {
        "label" => 'asms',
        "provider" => 'asms_provider',
        "description"=>'Activity Streams Engine',
        "free" => true,
        "version" => "2.0",
        "extra" => service_extra,
        "info_url" => "http://example.com/asms_dev",
        "external_id" => "jabba",
        "plans" => [
          {
            "external_id" => "thehutt",
            "id" => plan_name,
            "description" => "Free",
            "free" => true,
            "extra" => plan_extra,
          }
        ],
      }
    }
    let(:service_key) { "asms_asms_provider" }
    let(:plan_name) { "free" }
    let(:service_extra) { nil }
    let(:plan_extra) { "extra information" }

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

    let(:appdirect_marketplace) do
      described_class.new(
        cc_api_version: 'v2',
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
      let(:get_catalog) { appdirect_marketplace.get_catalog }

      it "does the mapping from objects to primitives" do
        get_catalog.should_not be_nil
        get_catalog.should have(1).keys

        asms_service = get_catalog.fetch("asms_asms_provider")
        asms_service["id"].should == "asms"
        asms_service["version"].should == "2.0"
        asms_service["description"].should == "Activity Streams Engine"
        asms_service["info_url"].should == "http://example.com/asms_dev"
        asms_service["provider"].should == "asms_provider"
        asms_service["unique_id"].should == "jabba"
      end

      it "contains plans information" do
        asms_service = get_catalog["asms_asms_provider"]
        asms_service['plans'].should == {
          "free" => {
            unique_id: "thehutt",
            description: "Free",
            free: true,
            extra: "extra information".to_json
          }
        }
      end

      describe "the service's extra field" do
        subject { get_catalog.fetch(service_key).fetch('extra') }

        context "when the service extra field is not nil" do
          let(:service_extra) { {"something" => "here"} }
          it { should ==  '{"something":"here"}'}
        end

        context "when the service extra field is nil" do
          let(:service_extra) { nil }
          it { should be_nil }
        end
      end

      describe "the plans' extra field" do
        subject { get_catalog.fetch(service_key).fetch('plans').fetch(plan_name).fetch(:extra) }

        context "when the plan extra field has a value" do
          let(:plan_extra) { {"iam" => "a value"} }
          it { should ==  '{"iam":"a value"}' }
        end

        context "when the plan extra field is nil" do
          let(:plan_extra) { nil }
          it { should be_nil }
        end
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
          organization_guid: 'organization-guid',
          unique_id: "snowflake#{rand(1000)}"
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
          opts['configuration']['plan']['external_id'].should == request.unique_id
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
end
