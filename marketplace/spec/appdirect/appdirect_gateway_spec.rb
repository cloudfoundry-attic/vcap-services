# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require 'spec_helper'
require_relative '../../lib/base/marketplace_async_gateway'
require_relative '../../lib/marketplaces/appdirect/appdirect_helper'
require_relative '../../lib/marketplaces/appdirect/appdirect_marketplace'

module VCAP
  module Services
    module Marketplace
      class MarketplaceAsyncServiceGateway
        attr_reader :logger
      end
    end
  end
end

describe "AppDirect Gateway" do

# Tempoarily disabling these tests until we figure out a better way of waiting for the app to initialize before running the test

=begin

  before :all do
    @config = load_config
    @gateway = VCAP::Services::Marketplace::MarketplaceAsyncServiceGateway.new(@config)
    puts "\n\object.methods : "+ @gateway.methods.sort.join("\n").to_s+"\n\n"

    while @gateway.ready_to_serve == false
      puts "Initializing..."
      sleep 1
    end

    @app_session = Rack::Test::Session.new(Rack::MockSession.new(@gateway))

    @rack_env = {
      "CONTENT_TYPE" => Rack::Mime.mime_type('.json'),
      "HTTP_X_VCAP_SERVICE_TOKEN" =>  @config[:token],
    }
    @api_version = "poc"
    @api = "#{@config[:appdirect][:scheme]}://#{@config[:appdirect][:host]}/api"
  end

  before do
    stub_fixture(:get, @api, VCAP::Services::Marketplace::Appdirect::AppdirectHelper::OFFERINGS_PATH, "urbanairship/")
    stub_cc_request(:post, "services/v1/offerings", "urbanairship/")
    stub_fixture(:post, @api, VCAP::Services::Marketplace::Appdirect::AppdirectHelper::SERVICES_PATH, "urbanairship/")
  end


  it "should add the 2 service offerings" do
    EM.run do
      @app_session.get "/", params = {}, rack_env = @rack_env
      last_response = @app_session.last_response
      last_response.should be_ok
      puts "LAST RESPONSE = #{last_response.inspect}"
      json = JSON.parse(last_response.body)
      json["offerings"].keys.should include "mongolab"
      json["offerings"].keys.should include "urbanairship"
      EM.stop
    end
  end

  it "should return 400 unless token" do
    EM.run do
      get "/", params = {}, rack_env = {}
      puts "LAST RESPONSE = #{last_response.inspect}"
      last_response.status.should == 400
      last_response.should_not be_ok
      EM.stop
    end
  end

  it "should respond to create_service" do
    EM.run do
      @svc_params = {
        :label => "mongolab-2.0",
        :name => "mymongo",
        :plan => "small",
        :email => "mwilkinson@vmware.com"
      }
      post "/gateway/v1/configurations", params = @svc_params.to_json, rack_env = @rack_env

      puts last_response.body.inspect
      last_response.should be_ok
      json = JSON.parse(last_response.body)
      EM.stop
    end
  end
=end

end
