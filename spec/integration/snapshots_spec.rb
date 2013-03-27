require 'spec_helper'

describe 'Snapshot features' do
  let!(:service_instance_guid) { provision_mysql_instance('mysql') }
  let(:timeout) {60}

  it 'can create an empty snapshot', :components => [:ccng, :mysql] do
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

  it 'can list snapshots when there are none', :components => [:ccng, :mysql] do
    response_body = ccng_get("/v2/service_instances/#{service_instance_guid}/snapshots")
    response_body['resources'].should be_empty
  end

  it 'can populate an empty snapshot for mysql', :components => [:ccng, :mysql, :mysql_worker] do
    # create an empty snapshot
    create_response = ccng_post("/v2/snapshots", {name: 'my_snapshot', service_instance_guid: service_instance_guid})
    created_snapshot_guid = create_response.fetch('metadata').fetch('guid') or raise 'No Snapshot GUID'

    # populate the empty snapshot
    populate_url = "/v2/snapshots/#{service_instance_guid}_#{created_snapshot_guid}/import_from_service_instance"
    populate_snapshot = ccng_put(populate_url, {})

    #status should be populating
    snapshot = ccng_get("/v2/service_instances/#{service_instance_guid}/snapshots")
    snapshot['resources'].first.fetch('entity').fetch('state').should == 'populating'
  end
end
