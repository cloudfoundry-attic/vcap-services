# Copyright (c) 2009-2011 VMware, Inc.
require File.dirname(__FILE__) + "/spec_helper"

require "rabbit_service/rabbit_node"

module VCAP
  module Services
    module Rabbit
      class Node
         attr_reader :local_ip, :base_dir, :node_id, :capacity, :max_clients, :rabbitmq_server, :port_gap
         attr_accessor :logger, :local_db
      end
    end
  end
end

describe VCAP::Services::Rabbit::Node do

  before :all do
    @options = getNodeTestConfig
    @options.freeze
    FileUtils.mkdir_p(@options[:base_dir])

    # Setup code must be wrapped in EM.run
    EM.run do
      @node = VCAP::Services::Rabbit::Node.new(@options)
      EM.stop
    end
  end

  after :all do
    FileUtils.rm_f(@options[:local_db_file])
    FileUtils.rm_rf(@options[:base_dir])
    # Use %x to call shell command since ruby doesn't has pkill interface
    %x[pkill epmd]
  end

  before :each do
    @instance = VCAP::Services::Rabbit::Node::ProvisionedService.new
    @instance.name = UUIDTools::UUID.random_create.to_s
    @instance.plan = 1
    @instance.plan_option = ""
    @instance.vhost = "v" + UUIDTools::UUID.random_create.to_s.gsub(/-/, "")
    @instance.port = 15000
    @instance.admin_port = 55000
    @instance.admin_username = "au" + @node.generate_credential
    @instance.admin_password = "ap" + @node.generate_credential
    @instance.memory = @options[:memory]
  end

  describe "Node.initialize" do
    it "should set up node configuration" do
      @node.local_ip.should be
      @node.base_dir.should == @options[:base_dir]
      @node.node_id.should == @options[:node_id]
      @node.capacity.should == @options[:capacity]
      @node.max_clients.should == @options[:max_clients]
      @node.rabbitmq_server.should == @options[:rabbitmq_server]
      @node.local_db.should == @options[:local_db]
    end
  end

  describe "Node.start_db" do
    it "should fail when set local db with non-existed file argument" do
      @node.local_db = "sqlite3:/non_existed/non-existed.db"
      expect {@node.start_db}.should raise_error
      @node.local_db = @options[:local_db]
    end

    it "should setup local db with right arguments" do
      @node.start_db.should be
    end
  end

  describe "Node.start_provisioned_instances" do
    it "should check whether provisioned instance is running or not" do
      @instance.pid = @node.start_instance(@instance)
      @instance.running?.should == true
      @node.stop_instance(@instance)
      @instance.running?.should == false
    end

    it "should not start a new instance if the instance is already started when start all provisioned instances" do
      @instance.pid = @node.start_instance(@instance)
      @instance.memory = 1
      @instance.save
      @node.start_provisioned_instances
      instance = VCAP::Services::Rabbit::Node::ProvisionedService.get(@instance.name)
      instance.pid.should == @instance.pid
      @node.stop_instance(@instance)
      @instance.destroy
    end

    it "should start a instance if the instance is not started when start all provisioned instances" do
      credentials = @node.provision(:free)
      instance = VCAP::Services::Rabbit::Node::ProvisionedService.get(credentials["name"])
      old_pid = instance.pid
      sleep 3
      instance.kill
      @node.start_provisioned_instances
      instance = VCAP::Services::Rabbit::Node::ProvisionedService.get(credentials["name"])
      instance.pid.should_not == old_pid
      @node.unprovision(credentials["name"])
    end
  end

  describe "Node.announcement" do
    it "should send node announcement" do
      @node.announcement.should be
    end
  end

  describe "Node.provision" do
    before :all do
      @credentials = @node.provision(:free)
      sleep 1
    end

    after :all do
      @node.unprovision(@credentials["name"])
    end

    it "should access the instance using the credentials returned by successful provision" do
      amqp_start(@credentials).should == true
    end

    it "should not allow null credentials to access the instance" do
      credentials = @credentials.clone
      credentials["user"] = ""
      credentials["pass"] = ""
      expect {amqp_connect(credentials)}.should raise_error(AMQP::Error)
    end

    it "should not allow wrong credentials to access the instance" do
      credentials = @credentials.clone
      credentials["pass"] = "wrong_pass"
      expect {amqp_connect(credentials)}.should raise_error(AMQP::Error)
    end

    it "should send provision message when finish a provision" do
      @credentials["name"].should be
      @credentials["host"].should be
      @credentials["host"].should == @credentials["hostname"]
      @credentials["port"].should be
      @credentials["vhost"].should be
      @credentials["user"].should be
      @credentials["user"].should == @credentials["username"]
      @credentials["pass"].should be
      @credentials["pass"].should == @credentials["password"]
      @credentials["url"].should be
    end
  end

  describe "Node.unprovision" do
    before :all do
      @credentials = @node.provision(:free)
      sleep 1
      @node.unprovision(@credentials["name"])
      sleep 1
    end

    it "should not access the instance when doing unprovision" do
      expect {amqp_connect(@credentials)}.should raise_error(AMQP::Error)
    end

    it "should raise exception when unprovision an non-existed name" do
      expect {@node.unprovision("non-existed")}.should raise_error(VCAP::Services::Rabbit::RabbitError)
    end
  end

  describe "Node.bind" do
    before :all do
      @instance_credentials = @node.provision(:free)
      sleep 1
      @binding_credentials = @node.bind(@instance_credentials["name"])
      sleep 1
    end

    after :all do
      @node.unbind(@binding_credentials)
      sleep 1
      @node.unprovision(@instance_credentials["name"])
    end

    it "should access rabbitmq server use the returned credential" do
      amqp_start(@binding_credentials).should == true
    end

    it "should not allow null credentials to access the instance" do
      credentials = @binding_credentials.clone
      credentials["user"] = ""
      credentials["pass"] = ""
      expect {amqp_connect(credentials)}.should raise_error(AMQP::Error)
    end

    it "should not allow wrong credentials to access the instance" do
      credentials = @binding_credentials.clone
      credentials["pass"] = "wrong_pass"
      expect {amqp_connect(credentials)}.should raise_error(AMQP::Error)
    end

    it "should send binding message when finish a binding" do
      @binding_credentials["host"].should be
      @binding_credentials["host"].should == @binding_credentials["hostname"]
      @binding_credentials["port"].should be
      @binding_credentials["vhost"].should be
      @binding_credentials["user"].should be
      @binding_credentials["user"].should == @binding_credentials["username"]
      @binding_credentials["pass"].should be
      @binding_credentials["pass"].should == @binding_credentials["password"]
      @binding_credentials["name"].should be
      @binding_credentials["url"].should be
    end
  end

  describe "Node.unbind" do
    before :all do
      @instance_credentials = @node.provision(:free)
      sleep 1
      @binding_credentials = @node.bind(@instance_credentials["name"])
      sleep 1
      @response = @node.unbind(@binding_credentials)
      sleep 1
      @node.unprovision(@instance_credentials["name"])
    end

    it "should not access rabbitmq server after unbinding" do
      expect {amqp_connect(@binding_credentials)}.should raise_error(AMQP::Error)
    end

    it "should return empty when unbinding successfully" do
      @response.should == {}
    end
  end

  describe "Node.save_instance" do
    it "should raise exception when save instance failed" do
      @instance.persisted_state = DataMapper::Resource::State::Immutable
      expect {@node.save_instance(@instance)}.should raise_error(VCAP::Services::Rabbit::RabbitError)
    end
  end

  describe "Node.destroy_instance" do
    it "should return true when the instance is in local db" do
      instance = VCAP::Services::Rabbit::Node::ProvisionedService.new
      instance.name = "test"
      instance.port = 1
      instance.plan = 1
      instance.vhost = "test"
      instance.port = 1
      instance.admin_port = 1
      instance.admin_username = "test"
      instance.admin_password = "test"
      instance.memory   = 1
      instance.save
      @node.destroy_instance(instance).should == true
      expect {@node.get_instance(instance.name)}.should raise_error(VCAP::Services::Rabbit::RabbitError)
    end

    it "should return true when the instance is not in local db" do
      instance = VCAP::Services::Rabbit::Node::ProvisionedService.new
      @node.destroy_instance(instance).should == true
      expect {@node.get_instance(instance.name)}.should raise_error(VCAP::Services::Rabbit::RabbitError)
    end
  end

  describe "Node.get_instance" do
    it "should raise exception when get instance failed" do
      expect {@node.get_instance("non-existed")}.should raise_error(VCAP::Services::Rabbit::RabbitError)
    end
  end

  describe "Node.varz_details" do
    it "should report varz details" do
      @credentials = @node.provision(:free)
      sleep 1
      varz = @node.varz_details
      varz[:provisioned_instances_num].should == 1
      varz[:provisioned_instances][0][:name].should == @credentials["name"]
      varz[:provisioned_instances][0][:vhost].should == @credentials["vhost"]
      varz[:provisioned_instances][0][:admin_username].should == @credentials["user"]
      varz[:provisioned_instances][0][:plan].should == "free"
      varz[:instances][@credentials["name"].to_sym].should == "ok"
      @node.unprovision(@credentials["name"])
    end
  end

  describe "check & purge orphan" do
    it "should return proper instances & bindings list" do
      before_instances = @node.all_instances_list
      before_bindings = @node.all_bindings_list
      oi = @node.provision(:free)
      ob = @node.bind(oi["name"], "rw")
      after_instances = @node.all_instances_list
      after_bindings = @node.all_bindings_list
      @node.unprovision(oi["name"], [])
      (after_instances - before_instances).include?(oi["name"]).should be_true
      (after_bindings - before_bindings).index { |credential| credential["username"] == ob["username"] }.should_not be_nil
    end

    it "should be able to purge the orphan" do
      oi = @node.provision("free")
      ob = @node.bind(oi["name"],'rw')
      @node.purge_orphan([oi["name"]],[ob])
      @node.all_instances_list.include?(oi["name"]).should be_false
      @node.all_bindings_list.index { |credential| credential["username"] == ob["username"] }.should be_nil
    end
  end

  describe "Node.migration" do
    before :all do
      @instance_credentials = @node.provision(:free)
      sleep 1
      @dump_dir = File.join("/tmp/migration/rabbit", @instance_credentials["name"])
      @binding_credentials1 = @node.bind(@instance_credentials["name"])
      @binding_credentials2 = @node.bind(@instance_credentials["name"])
      @binding_credentials_list = [@binding_credentials1, @binding_credentials2]
      @binding_credentials_map = {
        "credentials1" => {
          "binding_options" => nil,
          "credentials" => @binding_credentials1
        },
        "credentials2" => {
          "binding_options" => nil,
          "credentials" => @binding_credentials2
        },
      }
    end

    after :all do
      sleep 1
      @node.unprovision(@instance_credentials["name"], @binding_credentials_list)
    end

    it "should not access rabbitmq server after disable the instance" do
      @node.disable_instance(@instance_credentials, @binding_credentials_list)
      sleep 1
      @binding_credentials_list.each do |credentials|
        admin_credentials = {"username" => credentials["username"], "password" => credentials["password"], "admin_port" => credentials["port"] + @node.port_gap}
        expect {@node.get_permissions(admin_credentials, credentials["vhost"], credentials["user"])}.should raise_error(VCAP::Services::Rabbit::RabbitError)
      end
    end

    it "should dump db file to right location after dump instance" do
      @node.dump_instance(@instance_credentials, @binding_credentials_list, @dump_dir).should == true
    end

    it "should access rabbitmq server in old node after enable the instance" do
      @node.enable_instance(@instance_credentials, @binding_credentials_map)
      @binding_credentials_list.each do |credentials|
        amqp_start(credentials).should == true
      end
      sleep 1
    end

    it "should import db file from right location after import instance" do
      @node.unprovision(@instance_credentials["name"], @binding_credentials_list)
      sleep 1
      new_port = 11111
      @instance_credentials["port"] = new_port
      @binding_credentials_list.each do |credentials|
        credentials["port"] = new_port
      end
      @node.import_instance(@instance_credentials, @binding_credentials_map, @dump_dir, :free)
      sleep 1
      credentials_list = @node.enable_instance(@instance_credentials, @binding_credentials_map)
      credentials_list.size.should == 2
      amqp_start(credentials_list[0]).should == true
      credentials_list[1].each do |key, value|
        amqp_start(value["credentials"]).should == true
      end
    end
  end

  describe "Node.restart" do
    it "should still use the provisioned service after the restart" do
      credentials = @node.provision(:free)
      sleep 3
      EM.run do
        @node.shutdown
        @node = VCAP::Services::Rabbit::Node.new(@options)
        EM.add_timer(1) {EM.stop}
      end
      amqp_start(credentials).should == true
      @node.unprovision(credentials["name"])
    end
  end

  describe "Node.shutdown" do
    it "should return true when shutdown finished" do
      EM.run do
        @node.shutdown.should be
        EM.stop
      end
    end
  end

end
