# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require 'spec_helper'
require 'rack/test'

describe 'Serialization data server -- web server' do
  include Rack::Test::Methods

  def app
    @server = VCAP::Services::Serialization::Server.new(@opts)
  end

  before :all do
    @opts = load_config
    @opts[:serialization_base_dir] = "/tmp/spec_test_sds"
    @opts[:nginx] = nil

    @service = "mysql"
    @service_id = "abcd12349999"
    @rack_env = {
      "HTTP_X_VCAP_SDS_UPLOAD_TOKEN" => @opts[:upload_token]
    }
  end

  it "fake test for serialization data server " do
    expect {1.should == 1}
  end

  after :all do
    FileUtils.rm_rf("/tmp/sds_server.*")
    FileUtils.rm_rf("/tmp/spec_test_sds")
  end
end
