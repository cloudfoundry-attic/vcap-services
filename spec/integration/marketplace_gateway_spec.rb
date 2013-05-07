require 'spec_helper'
require 'json'

describe 'Marketplace Gateway - AppDirect integration', components: [:ccng, :marketplace] do
  let!(:services_response) do
    wait_for('/v2/services') do |response|
      response.fetch('resources').size == 3
    end
  end

  it 'populates CC with AppDirect services' do
    mongo_service = find_service_from_response(services_response, 'mongolab')
    mongo_service.fetch('provider').should == 'objectlabs'

    mongo_service.fetch('unique_id').should == '8'
    mongo_service.fetch('extra').should_not be_nil

    extra_information = JSON.parse(mongo_service.fetch('extra'))

    extra_information.fetch('provider').fetch('name').should == 'ObjectLabs'
    extra_information.fetch('listing').fetch('imageUrl').should == "https://example.com/profileImageUrl"
    extra_information.fetch('listing').fetch('blurb').should == "MongoDB is WEB SCALE"

    plans_url = mongo_service.fetch("service_plans_url")

    mongo_plans = get_contents(plans_url)
    mongo_plans.fetch("total_results").should eq(2)

    first_plan = mongo_plans.fetch('resources').first.fetch('entity')
    first_plan.fetch('unique_id').should == "addonOffering_98"
    first_plan.fetch('name').should == 'free'

    second_plan = mongo_plans.fetch('resources')[1].fetch('entity')
    second_plan.fetch('unique_id').should == "addonOffering_99"
    second_plan.fetch('name').should == "small"

    mongo_plans.fetch('resources').first.fetch('entity').fetch('extra').should be

    sendgrid_service = find_service_from_response(services_response, 'SendGrid')
    sendgrid_plans = get_contents(sendgrid_service.fetch('service_plans_url'))
    sendgrid_plans.fetch("total_results").should eq(1)
    sendgrid_plan_names = sendgrid_plans.fetch("resources").map {|r| r.fetch("entity").fetch("name")}
    sendgrid_plan_names.should == ["SENDGRID"]
    sendgrid_plans.fetch('resources').first.fetch('entity').fetch('extra').should be
  end

  it "can provision a service instance" do
    ccng_guid = provision_service_instance('awsome mongo', 'mongolab-dev', 'free')

    ccng_guid.should_not be_nil
    ccng_service_instance_guids.should include(ccng_guid)

    app_direct_service_instances = get_json('http://localhost:9999/test/provisioned_services')
    app_direct_service_instances.should have(1).entry
    app_direct_service_instances.first.fetch('space').fetch('uuid').should == space_guid
    app_direct_service_instances.first.fetch('space').fetch('organization').fetch('uuid').should == org_guid
    app_direct_service_instances.first.fetch('offering').fetch('label').should == 'mongodb'
    app_direct_service_instances.first.fetch('offering').fetch('provider').should == 'mongolab-dev'
    app_direct_service_instances.first.fetch('configuration').fetch('name').should == 'awsome mongo'
    app_direct_service_instances.first.fetch('configuration').fetch('plan').fetch('external_id').should == 'addonOffering_1' # from fixture
  end

  it "can unprovision a service instance" do
    ccng_guid = provision_service_instance('awsome mongo', 'mongolab-dev', 'free')
    app_direct_uuid = get_json('http://localhost:9999/test/provisioned_services').fetch(0).fetch('uuid')

    ccng_delete "/v2/service_instances/#{ccng_guid}"

    ccng_service_instance_guids.should_not include(ccng_guid)
    app_direct_destroyed_instances = get_json('http://localhost:9999/test/deprovisioned_services')
    app_direct_destroyed_instances.should have(1).entry
    app_direct_destroyed_instances.first.should == app_direct_uuid
  end

  def get_json(url)
    client = HTTPClient.new
    response = client.get(url)
    JSON.parse(response.body)
  end

  def ccng_service_instance_guids
    ccng_get('/v2/service_instances').fetch('resources').map { |r|
      r.fetch('metadata').fetch('guid')
    }
  end

  def find_service_from_response(response, service_label)
    response.fetch('resources').
      map {|resource| resource.fetch("entity")}.
      find {|entity| entity.fetch('label') == service_label } || raise("Not found service with specified label #{service_label}")
  end

  def get_contents(ccng_path)
    10.times do
      content = ccng_get(ccng_path)
      return content if content.fetch('resources').any?
      sleep 0.5
    end
    raise 'Did not have the contents after a while'
  end

  def wait_for(path, &predicate)
    10.times do
      content = ccng_get(path)
      if predicate.yield(content)
        return content
      end
      sleep 0.5
    end
    raise "predicate never satisfied"
  end
end
