# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require 'spec_helper'
require 'service_broker/async_gateway'

module VCAP
  module Services
    module ServiceBroker
      class AsynchronousServiceGateway
        attr_reader :logger
      end
    end
  end
end


describe "Service Broker" do
  include Rack::Test::Methods

  def app
    @gw = VCAP::Services::ServiceBroker::AsynchronousServiceGateway.new(@config)
  end

  before :all do
    @config = load_config
    @rack_env = {
      "CONTENT_TYPE" => Rack::Mime.mime_type('.json'),
      "HTTP_X_VCAP_SERVICE_TOKEN" =>  @config[:token],
    }
    @api_version = "poc"
  end

  it "should return bad request if request type is not json " do
    EM.run do
      get "/", params = {}, rack_env = {}
      last_response.status.should == 400
      EM.stop
    end
  end

  it "should return unauthorize error with mismatch token " do
    EM.run do
      @rack_env["HTTP_X_VCAP_SERVICE_TOKEN"] = "foobar"
      get "/", params = {}, rack_env = @rack_env
      last_response.status.should == 401
      EM.stop
    end
  end

end
