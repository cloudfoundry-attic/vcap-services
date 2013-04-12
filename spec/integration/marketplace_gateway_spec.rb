require 'spec_helper'
require 'json'

describe 'Marketplace Gateway - AppDirect integration' do
  context "some services are already loaded in CC" do
    it 'does not screw up the existing services'
    it 'updates existing services as needed'
  end

  it "the market gateways populate CC only with whitelisted services"

  it 'populates CC with AppDirect services', components: [:ccng, :marketplace]  do
    services_response = nil
    # retry a few times, since the service may not advertise offerings immediately
    20.times do
      services_response = ccng_get('/v2/services')
      break if services_response.fetch('resources').any?
      sleep 0.5
    end

    services_response.fetch('resources').should have(1).entry
    service = services_response.fetch('resources').first.fetch('entity')
    service.fetch('label').should == 'mongodb'
    service.fetch('provider').should == 'mongolab'

    service.fetch('extra').should_not be_nil
    extra_information = JSON.parse(service.fetch('extra'))

    extra_information.fetch('provider').fetch('name').should == 'mongolab'
    extra_information.fetch('listing').fetch('imageUrl').should == "https://example.com/profileImageUrl"
    extra_information.fetch('listing').fetch('blurb').should == "MongoDB is WEB SCALE"

    plans_url = service.fetch("service_plans_url")

    plans = nil
    10.times do
      plans = ccng_get(plans_url)
      break if plans.fetch('resources').any?
      sleep 0.5
    end

    plans.fetch("total_results").should eq(2)
    plan_names = plans.fetch("resources").map {|r| r.fetch("entity").fetch("name")}
    plan_names.should match_array([
      "free",
      "small",
    ])
  end
end
