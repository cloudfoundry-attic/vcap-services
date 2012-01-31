# Copyright (c) 2009-2011 VMware, Inc.
require File.dirname(__FILE__) + '/spec_helper'

require 'rabbit_service/rabbit_node'

module VCAP
  module Services
    module Rabbit
      class Node
         attr_reader :rabbit_ctl, :rabbit_server, :local_ip, :available_memory, :max_memory, :mbus
         attr_accessor :logger, :local_db
      end
    end
  end
end

describe VCAP::Services::Rabbit::Node do
  before :all do
    @logger = Logger.new(STDOUT, "daily")
    @logger.level = Logger::ERROR
    @local_db_file = "/tmp/rabbit_node_" + Time.now.to_i.to_s + ".db"
    @options = {
      :logger => @logger,
      :rabbit_ctl => "rabbitmqctl",
      :rabbit_server => "rabbitmq-server",
      :rabbit_port => 5672,
      :ip_route => "127.0.0.1",
      :available_memory => 4096,
      :max_memory => 16,
      :node_id => "rabbit-node-1",
      :local_db => "sqlite3:" + @local_db_file,
      :mbus => "nats://localhost:4222"
    }
    @default_permissions = "'.*' '.*' '.*'"
    EM.run do
      @node = VCAP::Services::Rabbit::Node.new(@options)
      EM.add_timer(0.1) {EM.stop}
    end
  end

  after :all do
    FileUtils.rm_f(@local_db_file)
  end

  before :each do
    @instance = VCAP::Services::Rabbit::Node::ProvisionedService.new
    @instance.name = UUIDTools::UUID.random_create.to_s
    @instance.plan = :free
    @instance.plan_option = ""
    @instance.vhost = "v" + UUIDTools::UUID.random_create.to_s.gsub(/-/, "")
    @instance.admin_username = "au" + @node.generate_credential
    @instance.admin_password = "ap" + @node.generate_credential
    @instance.memory = @options[:memory]
  end

  describe "Node.initialize" do
    it "should set up a rabbit controller path" do
      @node.rabbit_ctl.should be @options[:rabbit_ctl]
    end
    it "should set up a rabbit server path" do
      @node.rabbit_server.should be @options[:rabbit_server]
    end
    it "should set up a local IP" do
      @node.local_ip.should be
    end
    it "should set up an available memory size" do
      @node.available_memory.should be
    end
    it "should set up a maximum memory size" do
      @node.max_memory.should be @options[:max_memory]
    end
    it "should setup a local database path" do
      @node.local_db.should be @options[:local_db]
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

  describe "Node.rabbitmqctl.vhost" do
    it "should add vhost successfully using right arguments" do
      @node.add_vhost("test_vhost")
      %x[#{@options[:rabbit_ctl]} list_permissions -p test_vhost 2> /dev/null].split(/\n/)[-1].should == "...done."
      @node.delete_vhost("test_vhost")
    end

    it "should raise exception when add vhost using wrong arguments" do
      @node.add_vhost("test_vhost")
      expect {@node.add_vhost("test_vhost")}.should raise_error(VCAP::Services::Rabbit::RabbitError)
      @node.delete_vhost("test_vhost")
    end

    it "should delete vhost successfully using right arguments" do
      @node.add_vhost("test_vhost")
      @node.delete_vhost("test_vhost")
      %x[#{@options[:rabbit_ctl]} list_permissions -p test_vhost 2> /dev/null].split(/\n/)[-1].should_not == "...done."
    end

    it "should raise exception when delete vhost using wrong arguments" do
      expect {@node.delete_vhost("test_vhost")}.should raise_error(VCAP::Services::Rabbit::RabbitError)
    end
  end

  describe "Node.rabbitmqctl.user" do
    it "should add user successfully using right arguments" do
      @node.add_user("test_user", "test_password")
      @node.list_users.index("test_user").should be
      @node.delete_user("test_user")
    end

    it "should raise exception when add user using wrong arguments" do
      @node.add_user("test_user", "test_password")
      expect {@node.add_user("test_user", "test_password")}.should raise_error(VCAP::Services::Rabbit::RabbitError)
      @node.delete_user("test_user")
    end

    it "should delete user successfully using right arguments" do
      @node.add_user("test_user", "test_password")
      @node.delete_user("test_user")
      @node.list_users.index("test_user").should be_nil
    end

    it "should raise exception when delete user using wrong arguments" do
      expect {@node.delete_vhost("test_user")}.should raise_error(VCAP::Services::Rabbit::RabbitError)
    end
  end

  describe "Node.rabbitmqctl.permissions" do
    before :all do
      @node.add_vhost("test_vhost")
      @node.add_user("test_user", "test_password")
    end

    after :all do
      @node.delete_user("test_user")
      @node.delete_vhost("test_vhost")
    end

    it "should set permissons successfully using right arguments" do
      @node.set_permissions("test_vhost", "test_user", @default_permissions)
      @node.get_permissions("test_vhost", "test_user").should == @default_permissions
      @node.clear_permissions("test_vhost", "test_user")
    end

    it "should raise exception when get permissons using wrong arguments" do
      expect {@node.set_permissions("test_vhost", "no_existed_user", @default_permissions)}.should raise_error(VCAP::Services::Rabbit::RabbitError)
    end

    it "should get permissons successfully using right arguments" do
      @node.set_permissions("test_vhost", "test_user", @default_permissions)
      @node.get_permissions("test_vhost", "test_user").should == @default_permissions
    end

    it "should raise exception when get permissions using wrong arguments" do
      expect {@node.get_permissions("test_vhost", "no_existed_user")}.should raise_error(VCAP::Services::Rabbit::RabbitError)
    end

    it "should clear permissons successfully using right arguments" do
      @node.set_permissions("test_vhost", "test_user", @default_permissions)
      @node.clear_permissions("test_vhost", "test_user")
      @node.get_permissions("test_vhost", "test_user").should == ""
    end

    it "should raise exception when clear permissons using wrong arguments" do
      expect {@node.clear_permissions("test_vhost", "no_existed_user")}.should raise_error(VCAP::Services::Rabbit::RabbitError)
    end
  end

  describe "Node.rabbitmqctl.stats" do
    before :all do
      @node.add_vhost("test_vhost")
    end

    after :all do
      @node.delete_vhost("test_vhost")
    end

    it "should list all users successfully using right arguments" do
      @node.list_users.should be
    end

    it "should raise exception when list users using wrong arguments" do
      node_name = ENV["RABBITMQ_NODENAME"]
      ENV["RABBITMQ_NODENAME"] = "no_existed_node"
      expect {@node.list_users}.should raise_error(VCAP::Services::Rabbit::RabbitError)
      ENV["RABBITMQ_NODENAME"] = node_name
    end

    it "should list all queues successfully using right arguments" do
      @node.list_queues("test_vhost").should be
    end

    it "should raise exception when list queues using wrong arguments" do
      node_name = ENV["RABBITMQ_NODENAME"]
      ENV["RABBITMQ_NODENAME"] = "no_existed_node"
      expect {@node.list_queues("test_vhost")}.should raise_error(VCAP::Services::Rabbit::RabbitError)
      ENV["RABBITMQ_NODENAME"] = node_name
    end

    it "should list all exchanges successfully using right arguments" do
      @node.list_exchanges("test_vhost").should be
    end

    it "should raise exception when list exchanges using wrong arguments" do
      node_name = ENV["RABBITMQ_NODENAME"]
      ENV["RABBITMQ_NODENAME"] = "no_existed_node"
      expect {@node.list_exchanges("test_vhost")}.should raise_error(VCAP::Services::Rabbit::RabbitError)
      ENV["RABBITMQ_NODENAME"] = node_name
    end

    it "should list all bindings successfully using right arguments" do
      @node.list_bindings("test_vhost").should be
    end

    it "should raise exception when list bindings using wrong arguments" do
      node_name = ENV["RABBITMQ_NODENAME"]
      ENV["RABBITMQ_NODENAME"] = "no_existed_node"
      expect {@node.list_bindings("test_vhost")}.should raise_error(VCAP::Services::Rabbit::RabbitError)
      ENV["RABBITMQ_NODENAME"] = node_name
    end
  end

  describe 'Node.announcement' do
    it "should send node announcement" do
      @node.announcement.should be
    end

    it "should send available_memory in announce message" do
      @node.announcement[:available_memory].should == @node.available_memory
    end
  end

  describe "Node.provision" do
    before :all do
      @old_memory = @node.available_memory
      @credentials = @node.provision(:free)
      sleep 1
    end

    after :all do
      @node.unprovision(@credentials["name"])
    end

    it "should access the instance using the credentials returned by successful provision" do
      AMQP.start(:host => @credentials["host"],
                 :port => @credentials["port"],
                 :vhost => @credentials["vhost"],
                 :user => @credentials["user"],
                 :pass => @credentials["pass"]) do |conn|
        conn.connected?.should == true
        AMQP.stop {EM.stop}
      end
    end

    it "should not allow null credentials to access the instance" do
      expect do
        EM.run do
          AMQP.connect(:host => @credentials["host"],
                     :port => @credentials["port"],
                     :vhost => @credentials["vhost"],
                     :user => "",
                     :pass => "")
        end
      end.should raise_error(AMQP::Error)
    end

    it "should not allow wrong credentials to access the instance" do
      expect do
        EM.run do
          AMQP.connect(:host => @credentials["host"],
                     :port => @credentials["port"],
                     :vhost => @credentials["vhost"],
                     :user => @credentials["user"],
                     :pass => "wrong_pass")
        end
      end.should raise_error(AMQP::Error)
    end

    it "should decrease available memory when finish a provision" do
      (@old_memory - @node.available_memory).should == @node.max_memory
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
    end
  end

  describe "Node.unprovision" do
    before :all do
      @credentials = @node.provision(:free)
      sleep 1
      @old_memory = @node.available_memory
      @node.unprovision(@credentials["name"])
      sleep 1
    end

    it "should not access the instance when doing unprovision" do
      expect do
        EM.run do
          AMQP.connect(:host => @credentials["host"],
                     :port => @credentials["port"],
                     :vhost => @credentials["vhost"],
                     :user => @credentials["user"],
                     :pass => @credentials["pass"])
        end
      end.should raise_error(AMQP::Error)
    end

    it "should decrease available memory when finish a provision" do
      (@node.available_memory - @old_memory).should == @node.max_memory
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
      AMQP.start(:host => @binding_credentials["host"],
                 :port => @binding_credentials["port"],
                 :vhost => @binding_credentials["vhost"],
                 :user => @binding_credentials["user"],
                 :pass => @binding_credentials["pass"]) do |conn|
        conn.connected?.should == true
        AMQP.stop {EM.stop}
      end
    end

    it "should not allow null credentials to access the instance" do
      expect do
        EM.run do
          AMQP.connect(:host => @binding_credentials["host"],
                     :port => @binding_credentials["port"],
                     :vhost => @binding_credentials["vhost"],
                     :user => "",
                     :pass => "")
        end
      end.should raise_error(AMQP::Error)
    end

    it "should not allow wrong credentials to access the instance" do
      expect do
        EM.run do
          AMQP.connect(:host => @binding_credentials["host"],
                     :port => @binding_credentials["port"],
                     :vhost => @binding_credentials["vhost"],
                     :user => @binding_credentials["user"],
                     :pass => "wrong_pass")
        end
      end.should raise_error(AMQP::Error)
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
      expect do
        EM.run do
          AMQP.connect(:host => @binding_credentials["host"],
                     :port => @binding_credentials["port"],
                     :vhost => @binding_credentials["vhost"],
                     :user => @binding_credentials["user"],
                     :pass => @binding_credentials["pass"])
        end
      end.should raise_error(AMQP::Error)
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

  describe "Node.destory_instance" do
    it "should raise exception when destroy instance failed" do
      instance = VCAP::Services::Rabbit::Node::ProvisionedService.new
      expect {@node.destroy_instance(instance)}.should raise_error(VCAP::Services::Rabbit::RabbitError)
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
      varz[:max_instances_num].should == @options[:available_memory] / @options[:max_memory]
      varz[:provisioned_instances][0][:name].should == @credentials["name"]
      varz[:provisioned_instances][0][:vhost].should == @credentials["vhost"]
      varz[:provisioned_instances][0][:admin_username].should == @credentials["user"]
      varz[:provisioned_instances][0][:plan].should == :free
      @node.unprovision(@credentials["name"])
    end
  end

  describe "Node.healthz_details" do
    it "should report healthz details" do
      @credentials = @node.provision(:free)
      sleep 1
      healthz = @node.healthz_details
      healthz[:self].should == "ok"
      healthz[@credentials["name"].to_sym].should == "ok"
      @node.unprovision(@credentials["name"])
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
        @node.get_permissions(credentials["vhost"], credentials["user"]).should == ""
      end
    end

    it "should dump db file to right location after dump instance" do
      @node.dump_instance(@instance_credentials, @binding_credentials_list, @dump_dir).should == true
    end

    it "should access rabbitmq server in old node after enable the instance" do
      @node.enable_instance(@instance_credentials, @binding_credentials_map)
      @binding_credentials_list.each do |credentials|
        AMQP.start(:host => credentials["host"],
                   :port => credentials["port"],
                   :vhost => credentials["vhost"],
                   :user => credentials["user"],
                   :pass => credentials["pass"]) do |conn|
          conn.connected?.should == true
          AMQP.stop {EM.stop}
        end
      end
      sleep 1
    end

    it "should import db file from right location after import instance" do
      @node.unprovision(@instance_credentials["name"], @binding_credentials_list)
      sleep 1
      test = @node.import_instance(@instance_credentials, @binding_credentials_map, @dump_dir, :free)
      sleep 1
      credentials_list = @node.enable_instance(@instance_credentials, @binding_credentials_map)
      credentials_list.size.should == 2
      AMQP.start(:host => credentials_list[0]["host"],
                 :port => credentials_list[0]["port"],
                 :vhost => credentials_list[0]["vhost"],
                 :user => credentials_list[0]["user"],
                 :pass => credentials_list[0]["pass"]) do |conn|
        conn.connected?.should == true
        AMQP.stop {EM.stop}
      end
      credentials_list[1].each do |key, value|
        AMQP.start(:host => value["credentials"]["host"],
                   :port => value["credentials"]["port"],
                   :vhost => value["credentials"]["vhost"],
                   :user => value["credentials"]["user"],
                   :pass => value["credentials"]["pass"]) do |conn|
          conn.connected?.should == true
          AMQP.stop {EM.stop}
        end
      end
    end
  end

  describe "Node.restart" do
    it "should still use the provisioned service after the restart" do
      EM.run do
        credentials = @node.provision(:free)
        @node.shutdown
        sleep 2
        @node = VCAP::Services::Rabbit::Node.new(@options)
        EM.add_timer(1) {
          AMQP.start(:host => credentials["host"],
                     :port => credentials["port"],
                     :vhost => credentials["vhost"],
                     :user => credentials["user"],
                     :pass => credentials["pass"]) do |conn|
            conn.connected?.should == true
            AMQP.stop {EM.stop}
          end
          @node.unprovision(credentials["name"])
          EM.stop
        }
      end
    end
  end

  describe "Node.shutdown" do
    it "should return true when shutdown finished" do
      EM.run do
        @node.shutdown.should be
        sleep 1
        %x[#{@options[:rabbit_ctl]} status 2> /dev/null].split(/\n/)[-1].should_not == "...done."
        EM.add_timer(0.1) {EM.stop}
      end
    end
  end

end
