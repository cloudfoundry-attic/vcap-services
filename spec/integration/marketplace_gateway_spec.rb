require 'spec_helper'

describe 'Marketplace Gateway - AppDirect integration' do
  it 'populates CC with AppDirect services', components: [:ccng, :marketplace]  do
    services_response = nil
    # retry a few times, since the service may not advertise offerings immediately
    10.times do
      services_response = ccng_get('/v2/services')
      break if services_response.fetch('resources').any?
      sleep 0.5
    end
    services_response.fetch('resources').should have(1).entry
    entity = services_response.fetch('resources').first.fetch('entity')
    entity.fetch('label').should == 'mongodb'
    entity.fetch('provider').should == 'mongolab'
  end
end
