# Copyright (c) 2009-2011 VMware, Inc.
require File.dirname(__FILE__) + "/spec_helper"

require "rabbit_service/rabbit_node"

module VCAP
  module Services
    module Rabbit
      class Node
         attr_reader :options, :free_ports, :capacity
         attr_accessor :logger, :local_db
      end
    end
  end
end

describe VCAP::Services::Rabbit::Node do

  before :all do
    @options = getNodeTestConfig
    FileUtils.mkdir_p(@options[:base_dir])
    FileUtils.mkdir_p(@options[:image_dir])
    FileUtils.mkdir_p(@options[:service_log_dir])
    FileUtils.mkdir_p(@options[:migration_nfs])

    # Setup code must be wrapped in EM.run
    EM.run do
      @node = VCAP::Services::Rabbit::Node.new(@options)
      EM.add_timer(1) {EM.stop}
    end
  end

  after :all do
    FileUtils.rm_f(@options[:local_db_file])
    FileUtils.rm_rf(@options[:service_log_dir])
    FileUtils.rm_rf(@options[:image_dir])
    FileUtils.rm_rf(@options[:migration_nfs])
    FileUtils.rm_rf(@options[:base_dir])
    # Use %x to call shell command since ruby doesn't has pkill interface
    %x[pkill epmd]
  end

  describe "Node.pre_send_announcement" do
    before :all do
      @old_capacity = @node.capacity
      @credentials = @node.provision(:free)
      sleep 3
      @node.shutdown
      EM.run do
        @node = VCAP::Services::Rabbit::Node.new(@options)
        EM.add_timer(1) {EM.stop}
      end
    end

    after :all do
      @node.unprovision(@credentials["name"])
    end

    it "should start provisioned instances before sending announcement" do
      instance = @node.get_instance(@credentials["name"])
      amqp_start(@credentials, instance).should == true
    end

    it "should decrease the capacity" do
      @node.capacity.should == @old_capacity - 1
    end
  end

  describe "Node.announcement" do
    it "should send node announcement" do
      @node.announcement.should be
    end

    it "should send available_capacity in announce message" do
      @node.announcement[:available_capacity].should == @node.capacity
    end
  end

  describe "Node.provision" do
    before :all do
      @credentials = @node.provision(:free)
      @instance = @node.get_instance(@credentials["name"])
      @admin_credentials = @node.gen_admin_credentials(@instance)
    end

    after :all do
      @node.unprovision(@credentials["name"])
    end

    it "should access the instance using the credentials returned by successful provision" do
      amqp_start(@credentials, @instance).should == true
    end

    it "should create monitoring user on provision" do
      monit_user = @node.list_users(@admin_credentials).find{|user| user["name"] == @credentials["monit_user"]}
      monit_user.should be
      monit_user["tags"].should == "monitoring"
    end

    it "should export admin port for instance handle" do
      @credentials["admin_port"].should be
      client = RestClient::Resource.new("http://#{@credentials["username"]}:#{@credentials["password"]}@#{VCAP.local_ip}:#{@credentials["admin_port"]}/api")
      expect {client["queues"].get}.should_not raise_error
    end


    it "should not allow null credentials to access the instance" do
      credentials = @credentials.clone
      credentials["user"] = ""
      credentials["pass"] = ""
      expect {amqp_connect(credentials, @instance)}.should raise_error(AMQP::Error)
    end

    it "should not allow wrong credentials to access the instance" do
      credentials = @credentials.clone
      credentials["pass"] = "wrong_pass"
      expect {amqp_connect(credentials, @instance)}.should raise_error(AMQP::Error)
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
      @instance = @node.get_instance(@credentials["name"])
      @node.unprovision(@credentials["name"])
    end

    it "should not access the instance when doing unprovision" do
      expect {amqp_connect(@credentials, @instance)}.should raise_error(AMQP::Error)
    end

    it "should raise exception when unprovision an non-existed name" do
      expect {@node.unprovision("non-existed")}.should raise_error(VCAP::Services::Base::Error::ServiceError)
    end
  end

  describe "Node.bind" do
    before :all do
      @instance_credentials = @node.provision(:free)
      @instance = @node.get_instance(@instance_credentials["name"])
      @binding_credentials = @node.bind(@instance_credentials["name"])
      @admin_credentials = @node.gen_admin_credentials(@instance)
    end

    after :all do
      @node.unbind(@binding_credentials)
      @node.unprovision(@instance_credentials["name"])
    end

    it "should be tagged with 'management' for binding user" do
      bind_user = @node.list_users(@admin_credentials).find{|user| user["name"] == @binding_credentials["username"]}
      bind_user.should be
      bind_user["tags"].should == "management"
    end

    it "should access rabbitmq server use the returned credential" do
      amqp_start(@binding_credentials, @instance).should == true
    end

    it "should not allow null credentials to access the instance" do
      credentials = @binding_credentials.clone
      credentials["user"] = ""
      credentials["pass"] = ""
      expect {amqp_connect(credentials, @instance)}.should raise_error(AMQP::Error)
    end

    it "should not allow wrong credentials to access the instance" do
      credentials = @binding_credentials.clone
      credentials["pass"] = "wrong_pass"
      expect {amqp_connect(credentials, @instance)}.should raise_error(AMQP::Error)
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
      @instance = @node.get_instance(@instance_credentials["name"])
      @binding_credentials = @node.bind(@instance_credentials["name"])
      @response = @node.unbind(@binding_credentials)
      @node.unprovision(@instance_credentials["name"])
    end

    it "should not access rabbitmq server after unbinding" do
      expect {amqp_connect(@binding_credentials, @instance)}.should raise_error(AMQP::Error)
    end

    it "should return empty when unbinding successfully" do
      @response.should == {}
    end
  end

  describe "Node.varz_details" do
    it "should report varz details" do
      @credentials = @node.provision(:free)
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

  describe "Check & purge orphan" do
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
      ob = @node.bind(oi["name"], "rw")
      @node.purge_orphan([oi["name"]], [ob])
      @node.all_instances_list.include?(oi["name"]).should be_false
      @node.all_bindings_list.index { |credential| credential["username"] == ob["username"] }.should be_nil
    end
  end

  describe "Node.migration" do
    before :all do
      @instance_credentials = @node.provision(:free)
      @instance = @node.get_instance(@instance_credentials["name"])
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
      @node.unprovision(@instance_credentials["name"], @binding_credentials_list)
    end

    it "should not access rabbitmq server after disable the instance" do
      @node.disable_instance(@instance_credentials, @binding_credentials_list)
      @binding_credentials_list.each do |credentials|
        admin_credentials = {"username" => credentials["username"], "password" => credentials["password"], "hostname" => @instance.ip}
        expect {@node.get_permissions(admin_credentials, credentials["vhost"], credentials["user"])}.should raise_error(RestClient::Unauthorized)
      end
    end

    it "should dump db file to right location after dump instance" do
      @node.dump_instance(@instance_credentials, @binding_credentials_list, @dump_dir).should == true
    end

    it "should access rabbitmq server in old node after enable the instance" do
      @node.enable_instance(@instance_credentials, @binding_credentials_map)
      @binding_credentials_list.each do |credentials|
        amqp_start(credentials, @instance).should == true
      end
    end

    it "should import db file from right location after import instance" do
      @node.unprovision(@instance_credentials["name"], @binding_credentials_list)
      new_port = 11111
      @instance_credentials["port"] = new_port
      @binding_credentials_list.each do |credentials|
        credentials["port"] = new_port
      end
      sleep 2 # Wait old instance folder to be deleted
      @node.import_instance(@instance_credentials, @binding_credentials_map, @dump_dir, :free)
      @instance = @node.get_instance(@instance_credentials["name"])
      credentials_list = @node.update_instance(@instance_credentials, @binding_credentials_map)
      credentials_list.size.should == 2
      amqp_start(credentials_list[0], @instance).should == true
      credentials_list[1].each do |key, value|
        amqp_start(value["credentials"], @instance).should == true
      end
    end
  end

  describe "Node.restart" do
    it "should still use the provisioned service after the restart" do
      credentials = @node.provision(:free)
      sleep 3
      @node.shutdown
      EM.run do
        @node = VCAP::Services::Rabbit::Node.new(@options)
        EM.add_timer(1) {EM.stop}
      end
      instance = @node.get_instance(credentials["name"])
      amqp_start(credentials, instance).should == true
      @node.unprovision(credentials["name"])
    end

    it "should change loop file size if the configuration changed after restart" do
      credentials = @node.provision(:free)
      sleep 3
      [@options[:max_disk] * 2, @options[:max_disk]].each do |to_size|
        @node.shutdown
        @options[:max_disk] = to_size
        EM.run do
          @node = VCAP::Services::Rabbit::Node.new(@options)
          EM.add_timer(1) {EM.stop}
        end
        instance = @node.get_instance(credentials["name"])
        amqp_start(credentials, instance).should == true
        File.size(instance.image_file).should == to_size.to_i * 1024 * 1024
      end

      # Verify revert case if set to the wrong size
      @node.shutdown
      old_size = @options[:max_disk].to_i
      # Set disk size to 1M which is less than the size needed for sure
      @options[:max_disk] = 1
      EM.run do
        @node = VCAP::Services::Rabbit::Node.new(@options)
        EM.add_timer(1) {EM.stop}
      end
      instance = @node.get_instance(credentials["name"])
      amqp_start(credentials, instance).should == true
      File.size(instance.image_file).should == old_size.to_i * 1024 * 1024
      @options[:max_disk] = old_size
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
