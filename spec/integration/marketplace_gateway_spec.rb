require 'spec_helper'
require 'json'

describe 'Marketplace Gateway - AppDirect integration' do
  it 'populates CC with AppDirect services', components: [:ccng, :marketplace]  do
    services_response = wait_for('/v2/services') do |response|
      response.fetch('resources').size == 2
    end

    mongo_service = find_service_from_response(services_response, 'mongodb')
    mongo_service.fetch('provider').should == 'mongolab'

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
