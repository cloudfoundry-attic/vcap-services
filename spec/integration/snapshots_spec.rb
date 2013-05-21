require 'spec_helper'
require "sequel"

describe 'Snapshot features' do
  before do
    login_to_ccng_as('12345', 'sre@vmware.com')
  end

  let!(:service_instance_guid) { provision_mysql_instance('mysql') }

  it 'can create an empty snapshot', :components => [:nats, :ccng, :mysql] do
    # curl against CloudController
    create_response = ccng_post("/v2/snapshots", {name: 'my_snapshot', service_instance_guid: service_instance_guid})
    created_snapshot_guid = create_response.fetch('metadata').fetch('guid') or raise 'No Snapshot GUID'
    create_response.fetch("entity").fetch("name").should == "my_snapshot"

    # Assert that the snapshot appears in the list
    list_response = ccng_get("/v2/service_instances/#{service_instance_guid}/snapshots")
    list_response['resources'].should have(1).entries
    list_response['resources'].first.fetch('metadata').fetch('guid').should == created_snapshot_guid
    list_response['resources'].first.fetch('entity').fetch('name').should == "my_snapshot"
  end

  it 'can list snapshots when there are none', :components => [:nats, :ccng, :mysql] do
    response_body = ccng_get("/v2/service_instances/#{service_instance_guid}/snapshots")
    response_body['resources'].should be_empty
  end
end
