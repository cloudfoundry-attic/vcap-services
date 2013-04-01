#--
# Cloud Foundry 2012.02.03 Beta
# Copyright (c) [2009-2012] VMware, Inc. All Rights Reserved.
#
# This product is licensed to you under the Apache License, Version 2.0 (the "License").
# You may not use this product except in compliance with the License.
#
# This product includes a number of subcomponents with
# separate copyright notices and license terms. Your use of these
# subcomponents is subject to the terms and conditions of the
# subcomponent's license, as noted in the LICENSE file.
#++

require 'spec_helper'
require 'service/provisioner'

module CF::UAA

  class TokenIssuer
    def client_credentials_grant
      TokenInfo.new(:token_type=>"Bearer", :access_token=>"FOO")
    end
  end

  class Scim

    class << self
      attr_accessor :simulate_fail
    end

    def add(type, info)
      if Scim.simulate_fail
        Scim.simulate_fail = false
        raise CF::UAA::TargetError
      end
      result = info.dup
      result.delete(:client_secret)
      result
    end

    def put(type, info)
      info
    end

    def delete(type, id)
    end

    def get(type, id)
      {
        :client_id => id,
        :redirect_uri => ['https://uaa.cloudfoundry.com/redirect/client'],
        :owner=>'foo@bar.com'
      }
    end

  end

  module Http

    private

    def http_post(target, path=nil, body=nil, headers={})
      [200, '', {"location"=>'https://uaa.cloudfoundry.com/redirect/client#access_token=TOKEN'}]
    end

    def json_get(target, path=nil, authorization=nil, headers={})
      [{:name=>"app", :uris=>['foo.api.vcap.me']}]
    end

  end

end

module CF::UAA::OAuth2Service

  describe Provisioner do

    include SpecHelper

    before :each do
      CF::UAA::Scim.simulate_fail = false
      EM.run do
        @provisioner = Provisioner.new({:service=>service_config,:logger=>logger,:additional_options=>{},:cc_api_version=>"v2"})
        EM.stop
      end
    end

    subject { @provisioner }

    it "should have a service name" do
      @provisioner.service_name.should_not be_nil
    end

    it "should generate credentials" do
      credentials = @provisioner.gen_credentials("foo", "foo@bar.com")
      credentials["client_id"].should == "foo"
    end

    it "should recover from failure" do
      CF::UAA::Scim.simulate_fail = true
      credentials = @provisioner.gen_credentials("foo", "foo@bar.com")
      credentials["client_id"].should == "foo"
    end

    context "when provisioning" do

      it "should create a new client with credentials" do
        request = VCAP::Services::Api::GatewayProvisionRequest.new(:label=>"test-1.0", :name=>"test", :plan=>"free", :email=>"vcap_tester@vmware.com", :version=>"1.0")
        @provisioner.provision_service(request) do |svc|
          puts "Response: #{svc}"
          svc["success"].should be_true
          svc["response"][:configuration][:email].should == "vcap_tester@vmware.com"
          svc["response"][:credentials].should_not be_nil
          svc["response"][:credentials]["auth_server_url"].should_not be_nil
          svc["response"][:credentials]["client_id"].should_not be_nil
        end
      end

    end

    context "when provisioned" do

      before :each do

        request = VCAP::Services::Api::GatewayProvisionRequest.new(:label=>"test-1.0", :name=>"test", :plan=>"free", :email=>"vcap_tester@vmware.com", :version=>"1.0")

        @instance_id == nil
        @provisioner.provision_service(request) do |svc|
          @instance_id = svc["response"][:service_id]
        end

        @instance_id.should_not be_nil

        @handle_id == nil
        @provisioner.bind_instance(@instance_id, {}) do |svc|
          @handle_id = svc["response"][:service_id]
        end

        @handle_id.should_not be_nil

      end

      it "should fail on non-existent service" do
        @provisioner.unprovision_service("foo") do |svc|
          svc["success"].should be_false
        end
      end

      it "should remove existing service successfully" do
        @provisioner.unprovision_service(@instance_id) do |svc|
          svc["success"].should be_true
        end
      end

      it "should be able to bind" do

        @provisioner.bind_instance(@instance_id, {}) do |svc|
          svc["success"].should be_true
          svc["response"][:configuration][:email].should == "vcap_tester@vmware.com"
        end

      end

      it "should be able to unbind" do

        @provisioner.unbind_instance(@instance_id, @handle_id, {}) do |svc|
          svc["success"].should be_true
        end

      end

      it "should be able to remove bindings when unprovisioning" do

        @provisioner.bind_instance(@instance_id, {}) {}
        @provisioner.unprovision_service(@instance_id) do |svc|
          svc["success"].should be_true
        end

        @provisioner.find_instance_bindings(@instance_id).should be_empty

      end

    end

  end

end
