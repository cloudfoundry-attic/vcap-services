require 'spec_helper'

describe 'Shared MySQL' do
  it 'can provision a service instance', components: [:ccng, :mysql] do
    ccng_get("/v2/spaces/#{space_guid}/service_instances").fetch("total_results").should == 0
    expect {
      provision_mysql_instance("yoursql")
      provision_mysql_instance("oursql")
    }.to change { mysql_root_connection["SHOW DATABASES"].to_a.size }.by(2)
    ccng_get("/v2/spaces/#{space_guid}/service_instances").fetch("total_results").should == 2
  end
end
