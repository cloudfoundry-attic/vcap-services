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
    @logger.level = Logger::DEBUG
    @local_db_file = "/tmp/rabbit_node_" + Time.now.to_i.to_s + ".db"
    @options = {
      :logger => @logger,
      :rabbit_ctl => "rabbitmqctl",
      :rabbit_server => "rabbitmq-server",
      :ip_route => "127.0.0.1",
      :available_memory => 4096,
      :max_memory => 16,
      :node_id => "rabbit-node-1",
      :local_db => "sqlite3:" + @local_db_file,
      :mbus => "nats://localhost:4222"
    }

    # Start NATS server
    @uri = URI.parse(@options[:mbus])
    @pid_file = "/tmp/nats-rabbit-test.pid"
    if !NATS.server_running?(@uri)
      %x[ruby -S bundle exec nats-server -p #{@uri.port} -P #{@pid_file} -d 2> /dev/null]
    end
    sleep 1

    EM.run do
      @node = VCAP::Services::Rabbit::Node.new(@options)
      EM.add_timer(0.1) {EM.stop}
    end
  end

  after :all do
    # Stop NATS server
    if File.exists?(@pid_file)
      pid = File.read(@pid_file).chomp.to_i
      %x[kill -9 #{pid}]
      %x[rm -f #{@pid_file}]
    end
    %x[rm -f #{@local_db_file}]
  end

  before :each do
    @instance = VCAP::Services::Rabbit::Node::ProvisionedInstance.new
    @instance.name = "rabbit-#{UUIDTools::UUID.random_create.to_s}"
    @instance.plan = :free
    @instance.plan_option = ""
    @instance.vhost = "v" + UUIDTools::UUID.random_create.to_s.gsub(/-/, "")
    @instance.admin_username = "au" + @node.generate_credential
    @instance.admin_password = "ap" + @node.generate_credential
    @instance.memory = @options[:memory]
  end

  describe "Node.initialize" do
    it "should set up a rabbit controler path" do
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
      @node.logger.level = Logger::ERROR
      begin
        @node.start_db
      rescue => e
        e.should be
      end
      @node.logger.level = Logger::DEBUG
      @node.local_db = @options[:local_db]
    end

    it "should setup local db with right arguments" do
      @node.start_db.should be
    end
  end

  describe "Node.start_server" do
    it "should start rabbit server correctly" do
      @node.start_server.should be
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

    it "should access the instance instance using the credentials returned by sucessful provision" do
      EM.run do
        AMQP.start(:host => @credentials["hostname"],
                   :vhost => @credentials["vhost"],
                   :user => @credentials["user"],
                   :pass => @credentials["pass"]) do |conn|
          conn.connected?.should == true
        end
        AMQP.stop
        EM.add_timer(1) {EM.stop}
      end
    end

    it "should not allow null credentials to access the instance instance" do
      EM.run do
        AMQP.start(:host => @credentials["hostname"],
                   :vhost => @credentials["vhost"],) do |conn|
          conn.connected?.should == false
        end
        AMQP.stop
        EM.add_timer(1) {EM.stop}
      end
    end

    it "should not allow wrong credentials to access the instance instance" do
      EM.run do
        AMQP.start(:host => @credentials["hostname"],
                   :vhost => @credentials["vhost"],
                   :user => @credentials["user"],
                   :pass => "wrong_pass") do |conn|
          conn.connected?.should == false
        end
        AMQP.stop
        EM.add_timer(1) {EM.stop}
      end
    end

    it "should decrease available memory when finish a provision" do
      (@old_memory - @node.available_memory).should == @node.max_memory
    end

    it "should send provision messsage when finish a provision" do
      @credentials["name"].should be
      @credentials["hostname"].should be
      @credentials["vhost"].should be
      @credentials["user"].should be
      @credentials["pass"].should be
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

    it "should not access the instance instance when doing unprovision" do
      EM.run do
        AMQP.start(:host => @credentials["hostname"],
                   :vhost => @credentials["vhost"],
                   :user => @credentials["user"],
                   :pass => @credentials["pass"]) do |conn|
          conn.connected?.should == false
        end
        AMQP.stop
        EM.add_timer(1) {EM.stop}
      end
    end

    it "should decrease available memory when finish a provision" do
      (@node.available_memory - @old_memory).should == @node.max_memory
    end

    it "should raise error when unprovision an non-existed name" do
      begin
        @node.unprovision("non-existed")
      rescue => e
        e.class.should == VCAP::Services::Rabbit::RabbitError
      end
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

    it "should access redis server use the returned credential" do
      EM.run do
        AMQP.start(:host => @binding_credentials["hostname"],
                   :vhost => @binding_credentials["vhost"],
                   :user => @binding_credentials["user"],
                   :pass => @binding_credentials["pass"]) do |conn|
          conn.connected?.should == false
        end
        AMQP.stop
        EM.add_timer(1) {EM.stop}
      end
    end

    it "should not allow null credentials to access the instance instance" do
      EM.run do
        AMQP.start(:host => @binding_credentials["hostname"],
                   :vhost => @binding_credentials["vhost"],) do |conn|
          conn.connected?.should == false
        end
        AMQP.stop
        EM.add_timer(1) {EM.stop}
      end
    end

    it "should not allow wrong credentials to access the instance instance" do
      EM.run do
        AMQP.start(:host => @binding_credentials["hostname"],
                   :vhost => @binding_credentials["vhost"],
                   :user => @binding_credentials["user"],
                   :pass => "wrong_pass") do |conn|
          conn.connected?.should == false
        end
        AMQP.stop
        EM.add_timer(1) {EM.stop}
      end
    end

    it "should send binding messsage when finish a binding" do
      @binding_credentials["hostname"].should be
      @binding_credentials["vhost"].should be
      @binding_credentials["user"].should be
      @binding_credentials["pass"].should be
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

    it "should not access redis server after unbinding" do
      EM.run do
        AMQP.start(:host => @binding_credentials["hostname"],
                   :vhost => @binding_credentials["vhost"],
                   :user => @binding_credentials["user"],
                   :pass => @binding_credentials["pass"]) do |conn|
          conn.connected?.should == false
        end
        AMQP.stop
        EM.add_timer(1) {EM.stop}
      end
    end

    it "should return empty when unbinding successfully" do
      @response.should == {}
    end
  end

  describe "Node.save_instance" do
    it "shuold raise error when save instance instance failed" do
      @instance.persisted_state=DataMapper::Resource::State::Immutable
      begin
        @node.save_instance(@instance)
      rescue => e
        e.class.should == VCAP::Services::Rabbit::RabbitError
      end
    end
  end

  describe "Node.destory_instance" do
    it "shuold raise error when destroy instance instance failed" do
      begin
        instance = VCAP::Services::Rabbit::Node::ProvisionedInstance.new
        @node.destroy_instance(instance)
      rescue => e
        e.class.should == VCAP::Services::Rabbit::RabbitError
      end
    end
  end

  describe "Node.get_instance" do
    it "shuold raise error when get instance instance failed" do
      begin
        @node.get_instance("non-existed")
      rescue => e
        e.class.should == VCAP::Services::Rabbit::RabbitError
      end
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

  describe "Node.shutdown" do
    it "should return true when shutdown finished" do
      EM.run do
        @node.shutdown.should be
        sleep 1
        %x[#{@options[:rabbit_ctl]} status].split(/\n/)[-1].should_not == "...done."
        EM.add_timer(0.1) {EM.stop}
      end
    end
  end

end
