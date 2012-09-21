# Copyright (c) 2009-2011 VMware, Inc.

require 'eventmachine'

$:.unshift(File.dirname(__FILE__))
require_relative '../spec_helper'
require 'helpers'
require_relative '../do'
require_relative '../../lib/base/marketplace_async_gateway'
require_relative '../../lib/marketplaces/test/test_marketplace'

describe "MarketplaceGateway" do

  include Do

  it "should add service offerings from marketplace" do
    EM.run do
      cc = nil
      gw = nil
      client = nil

      Do.at(0) {
        cc = MarketplaceGatewayHelper.create_cc
        gw = MarketplaceGatewayHelper.create_mpgw
        client = MarketplaceGatewayHelper.create_client
      }
      Do.at(2) { client.send_get_request("/") }
      Do.at(3) {
        client.last_http_code.should == 200
        json = JSON.parse(client.last_response)
        json["offerings"].keys.should include "testservice-1.0"
      }
      Do.at(4) { cc.stop; gw.stop; EM.stop }
    end
  end

  it "should return 401 unless token" do
    EM.run do
      cc = nil
      gw = nil
      client = nil
      old_token = nil

      Do.at(0) {
        cc = MarketplaceGatewayHelper.create_cc
        gw = MarketplaceGatewayHelper.create_mpgw
        client = MarketplaceGatewayHelper.create_client
        old_token = client.set_token("bad_token")
      }
      Do.at(2) { client.send_get_request("/") }
      Do.at(3) {
        client.last_http_code.should == 401
        client.set_token(old_token)
      }
      Do.at(4) { cc.stop; gw.stop; EM.stop }
    end
  end

  it "should be able to provision, bind, unbind, unprovision a service" do
    EM.run do
      cc = nil
      gw = nil
      client = nil

      Do.at(0) {
        cc = MarketplaceGatewayHelper.create_cc
        gw = MarketplaceGatewayHelper.create_mpgw
        client = MarketplaceGatewayHelper.create_client
      }

      Do.at(2) { client.send_provision_request("testservice-1.0", "test1", "foo@xyz.com", "small", "1.0") }
      Do.at(3) {
        client.last_http_code.should == 200
        json = JSON.parse(client.last_response)
        json.keys.should include "credentials"
        json.keys.should include "service_id"
      }

      Do.at(4) { client.send_bind_request("foo", "bar", "foo@xyz.com", {}) }
      Do.at(5) {
        client.last_http_code.should == 200
        json = JSON.parse(client.last_response)
        json.keys.should include "credentials"
        json.keys.should include "service_id"
      }

      Do.at(6) { client.send_unbind_request("foo", "bar") }
      Do.at(7) { client.last_http_code.should == 200 }

      Do.at(8) { client.send_unprovision_request("foo") }
      Do.at(9) { client.last_http_code.should == 200 }

      Do.at(10) { cc.stop; gw.stop; EM.stop }
    end
  end

  it "should expose varz and healthz" do
    EM.run do
      cc = nil
      gw = nil
      client = nil

      Do.at(0) {
        cc = MarketplaceGatewayHelper.create_cc
        gw = MarketplaceGatewayHelper.create_mpgw
        client = MarketplaceGatewayHelper.create_client
      }

      Do.at(2) { client.get_healthz }
      Do.at(3) {
        client.last_http_code.should == 200
        client.last_response.should == "ok\n"
      }

      Do.at(4) { varz = client.get_varz }
      Do.at(5) {
        client.last_http_code.should == 200
        varz = JSON.parse(client.last_response)
        varz.keys.should include "marketplace_gateway"
        varz["marketplace_gateway"]["disabled_services"].should == 0
        varz["marketplace_gateway"]["active_offerings"].should == 1

        varz.keys.should include "Test"
        varz["Test"]["available_services"].should == 1
      }

      Do.at(6) { cc.stop; gw.stop; EM.stop }
    end
  end

  it "should inactivate disabled offerings" do
    EM.run do
      cc = nil
      gw = nil
      client = nil

      Do.at(0) {
        cc = MarketplaceGatewayHelper.create_cc
        gw = MarketplaceGatewayHelper.create_mpgw
        client = MarketplaceGatewayHelper.create_client
      }

      Do.at(2) { client.set_config("enable_foo", "true") }
      Do.at(3) { client.last_http_code.should == 200 }

      Do.at(4) { varz = client.get_varz }
      Do.at(5) {
        client.last_http_code.should == 200
        varz = JSON.parse(client.last_response)
        varz["marketplace_gateway"]["disabled_services"].should == 0
        varz["marketplace_gateway"]["active_offerings"].should == 2
      }

      Do.at(6) { client.set_config("enable_foo", "false") }
      Do.at(7) { client.last_http_code.should == 200 }

      Do.at(8) { varz = client.get_varz }
      Do.at(9) {
        client.last_http_code.should == 200
        varz = JSON.parse(client.last_response)
        varz["marketplace_gateway"]["disabled_services"].should == 1
        varz["marketplace_gateway"]["active_offerings"].should == 1
      }

      Do.at(10) { cc.stop; gw.stop; EM.stop }
    end
  end

end
