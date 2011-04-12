# Copyright (c) 2009-2011 VMware, Inc.
require "spec_helper"
require "vcap/common"
require "mongodb_service/mongodb_provisioner"
require "mongodb_service/mongodb_node"
require "mongo"


include VCAP::Services::MongoDB

module VCAP
  module Services
    module MongoDB
      class Gateway
        #attr_reader :available_memory
      end
    end
  end
end


describe VCAP::Services::MongoDB::Provisioner do
  before :each do
    EM.run do
      @helper = ProvisionHelper.new
      EM.add_timer(1) do
        @helper.provision_service('1.6', 'free')
        EM.add_timer(1) do
          @helper.data.should_not be_nil
          @helper.service_id.should_not be_nil
          @helper.credentials.should_not be_nil
        end
      end
      EM.add_timer(4) do
        EM.stop
      end
    end
  end

  after :each do
    EM.run do
      @helper.unprovision()
      EM.add_timer(1) do
        @helper.success.should be_true
      end
      EM.add_timer(4) do
        EM.stop
      end
    end
  end


  #it "should be able to connect to the server" do
    #EM.run do
      #hostname = @helper.credentials["hostname"]
      #port     = @helper.credentials["port"]
      #conn = Mongo::Connection.new(hostname, port).db('local')
      ## TODO add authentication here.
      ## conn.authenticate(@resp[:admin], @resp[:adminpass])
      #coll = conn.collection('mongo_unit_test')
      #coll.insert({'a' => 1})
      #coll.count().should == 1
      #coll.drop
      #EM.stop
    #end
  #end

  #it "should allow bind token and revoke token" do
    #EM.run do
      #instance_id   = @helper.service_id
      #binding_token = "abcdefg"
      #app_id        = "aaaaaaa"
      #options       = {}
      #handle        = nil
      #@helper.sp.bind_instance(instance_id, binding_token, app_id, options) do |h|
        #handle = h
      #end
      ## TODO should move in block
      ## should be able to connect to db
      #EM.add_timer(1) do
        #puts handle
        #hostname = handle["credentials"]["hostname"]
        #port     = handle["credentials"]["port"]
        #db       = handle["credentials"]["db"]
        #username = handle["credentials"]["username"]
        #password = handle["credentials"]["password"]

        #conn = Mongo::Connection.new(hostname, port).db(db)
        #conn.authenticate(username, password)
        #coll = conn.collection('mongo_unit_test')
        #coll.insert({'a' => 1})
        #coll.count().should == 1
        #coll.drop


        #@helper.sp.unbind_instance(binding_token, app_id) do |r|
          #r.should be_true
        #end

        #EM.add_timer(1) do
          #conn = Mongo::Connection.new(hostname, port).db(db)
          #conn.should_not be_nil
          #begin
            #conn.authenticate(username, password)
          #rescue e
            #e.should_not be_nil
          #end
          #EM.stop
        #end
      #end
    #end
  #end

  class ProvisionHelper

    attr_reader :data, :service_id, :credentials, :success, :sp

    def initialize()
      @node_opts    = get_node_config()
      @gateway_opts = get_provisioner_config()
      start()
    end

    def start()
      @node = Node.new(@node_opts)
      @sp = Provisioner.new(@gateway_opts).start()
    end

    def provision_service(version, plan)
      @sp.provision_service(version, plan) do |svc|
        save_svc(svc)
      end
    end

    def unprovision()
      @sp.unprovision_service(@service_id) do |success|
        @success = success
      end
    end

    def save_svc(svc)
      @data = svc[:data]
      @service_id = svc[:service_id]
      @credentials = svc[:credentials]
    end

    def print()
      puts @data
      puts @service_id
      puts @credentials
    end

  end
end

