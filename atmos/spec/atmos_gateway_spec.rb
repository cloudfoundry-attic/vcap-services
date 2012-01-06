# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')
require File.dirname(__FILE__) + '/spec_helper'
require "atmos_service/provisioner"
require "atmos_service/atmos_helper"
require "uuidtools"

require "atmos_rest_client"

module VCAP
  module Services
    module Atmos
      class Provisioner
        attr_reader :prov_svcs
      end
    end
  end
end

include VCAP::Services::Atmos

describe VCAP::Services::Atmos::Provisioner do

  before :all do
    @run_tests = check_provisioner_config
    @config = get_provisioner_config
    @logger = @config[:logger]
    @logger.debug @config

    @atmos_helper = Helper.new(@config[:additional_options][:atmos], @logger)
  end

  it "should successfully new VCAP::Services::Atmos::Provisioner instance" do
    EM.run do
      sg = Provisioner.new(@config)
      @logger.debug sg
      sg.should_not be_nil
      EM.stop
    end
  end

  it "should handle local hash correctly when unprovision" do
    EM.run do
      sg = Provisioner.new(@config)
      sg.should_not be_nil
      sg.prov_svcs.empty?.should == true
      #
      svc_local = {
        :configuration => {'subtenant_name' => 'st_name', 'subtenant_id' => 'st_id', 'host' => 'host'},
        :service_id => 'st_name',
        :credentials => {'host' => 'host', 'port' => 'port', 'token' => 'token',
          'shared_secret' => 'shared_secret', 'subtenant_id' => 'st_id'}
      }
      sg.prov_svcs[svc_local[:service_id]] = svc_local
      binding_local = {
        :service_id => 'token',
        :configuration => svc_local[:configuration],
        :credentials => {'host' => 'host', 'port' => 'port', 'token' => 'token',
          'shared_secret' => 'shared_secret', 'subtenant_id' => svc_local[:configuration]['subtenant_id']}
      }
      sg.prov_svcs[binding_local[:service_id]] = binding_local
      sg.prov_svcs.count.should == 2
      #
      sg.remove_local_bindings svc_local[:service_id]
      sg.prov_svcs.count.should == 1
      sg.prov_svcs.delete svc_local[:service_id]
      sg.prov_svcs.empty?.should == true
      #
      EM.stop
    end
  end

  describe "provision_bind_unbind" do
    before :all do
      @subtenant_name_p = UUIDTools::UUID.random_create.to_s
      @subtenant_name_p1 = UUIDTools::UUID.random_create.to_s
      @token = UUIDTools::UUID.random_create.to_s
    end

    it "should successfully create atmos subtenant" do
      subtenant_id = @atmos_helper.create_subtenant(@subtenant_name_p)
      subtenant_id.should_not be_nil
    end

    it "should successfully create token under a subtenant" do
      shared_secret = @atmos_helper.create_user(@token, @subtenant_name_p)
      @logger.debug "token: " + @token + ", shared_secret: " + shared_secret
      shared_secret.should_not be_nil
    end

    it "should successfully delete token under a subtenant" do
      success = @atmos_helper.delete_user(@token, @subtenant_name_p)
      success.should == true
    end

    it "should successfully create object after bind" do
      subtenant_id = @atmos_helper.create_subtenant(@subtenant_name_p1)
      subtenant_id.should_not be_nil
      shared_secret = @atmos_helper.create_user(@token, @subtenant_name_p1)
      @logger.debug "token: " + @token + ", shared_secret: " + shared_secret
      shared_secret.should_not be_nil
      host = @config[:additional_options][:atmos][:host]
      port = @config[:additional_options][:atmos][:port]

      opts = {
        :url => "http://" + host + ":" + port,
        :sid => subtenant_id,
        :uid => @token,
        :key => shared_secret,
      }
      client = AtmosClient.new(opts)
      obj = UUIDTools::UUID.random_create.to_s
      res = client.create_obj(obj)
      res.should_not == Net::HTTPForbidden

      id = res['location']
      @logger.debug "object: " + obj + " created at: #{id}"
      res = client.get_obj(id)
      res.should_not == Net::HTTPForbidden

      @logger.debug "response of reading object: #{res.body}"
      obj_same = obj == res.body
      obj_same.should == true

      res = client.delete_obj(id)
      res.should_not == Net::HTTPForbidden
      @logger.debug "response of deleting file: #{res}"
    end

    after :all do
      if @run_tests
        @atmos_helper.delete_subtenant(@subtenant_name_p)
        @atmos_helper.delete_subtenant(@subtenant_name_p1)
      end
    end
  end

  describe "multi-tenancy" do
    before :all do
      @subtenant_name1 = UUIDTools::UUID.random_create.to_s
      @subtenant_name2 = UUIDTools::UUID.random_create.to_s
      @token = UUIDTools::UUID.random_create.to_s
    end

    it "should isolate between different subtenants" do
      subtenant_id1 = @atmos_helper.create_subtenant(@subtenant_name1)
      subtenant_id2 = @atmos_helper.create_subtenant(@subtenant_name2)
      subtenant_id1.should_not be_nil
      subtenant_id2.should_not be_nil

      shared_secret1 = @atmos_helper.create_user(@token, @subtenant_name1)
      shared_secret2 = @atmos_helper.create_user(@token, @subtenant_name2)
      shared_secret1.should_not be_nil
      shared_secret2.should_not be_nil

      host = @config[:additional_options][:atmos][:host]
      port = @config[:additional_options][:atmos][:port]

      opts = {
        :url => "http://" + host + ":" + port,
        :sid => subtenant_id1,
        :uid => @token,
        :key => shared_secret2,
      }
      client = AtmosClient.new(opts)
      res = client.create_obj("obj")
      @logger.debug res.to_s
      same_class = res == Net::HTTPForbidden || res['location'].nil?
      same_class.should == true

      opts = {
        :url => "http://" + host + ":" + port,
        :sid => subtenant_id2,
        :uid => @token,
        :key => shared_secret1,
      }
      client = AtmosClient.new(opts)
      res = client.create_obj("obj")
      @logger.debug res.to_s
      same_class = res == Net::HTTPForbidden || res['location'].nil?
      same_class.should == true
    end

    after :all do
      if @run_tests
        @atmos_helper.delete_subtenant(@subtenant_name1)
        @atmos_helper.delete_subtenant(@subtenant_name2)
      end
    end
  end

  describe "null credential" do
    before :all do
      @subtenant_name = UUIDTools::UUID.random_create.to_s
    end

    it "should prevent null credential from login" do
      subtenant_id = @atmos_helper.create_subtenant(@subtenant_name)
      subtenant_id.should_not be_nil
      host = @config[:additional_options][:atmos][:host]
      port = @config[:additional_options][:atmos][:port]

      opts = {
        :url => "http://" + host + ":" + port,
        :sid => subtenant_id,
        :uid => "",
        :key => "",
      }
      client = AtmosClient.new(opts)
      res = client.create_obj("obj")
      @logger.debug res.to_s
      same_class = res == Net::HTTPForbidden || res['location'].nil?
      same_class.should == true
    end

    after :all do
      @atmos_helper.delete_subtenant(@subtenant_name) if @run_tests
    end
  end

  describe "unprovision" do
    before :all do
      @subtenant_name_up = UUIDTools::UUID.random_create.to_s
      @atmos_helper.create_subtenant(@subtenant_name_up) if @run_tests
    end

    it "should successfully delete atmos subtenant" do
      @logger.debug "subtenant_name: " + @subtenant_name_up
      success = @atmos_helper.delete_subtenant(@subtenant_name_up)
      success.should == true
    end
  end
end
