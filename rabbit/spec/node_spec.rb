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
    @options = {
      :logger => @logger,
      :rabbit_ctl => "rabbitmqctl",
      :rabbit_server => "rabbitmq-server",
      :ip_route => "127.0.0.1",
      :available_memory => 4096,
      :max_memory => 16,
      :node_id => "rabbit-node-1",
      :local_db => "sqlite3:/tmp/rabbit_node.db",
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
  end

  before :each do
    @service = VCAP::Services::Rabbit::Node::ProvisionedService.new
    @service.name = "rabbit-#{UUIDTools::UUID.random_create.to_s}"
    @service.plan = :free
    @service.plan_option = ""
    @service.vhost = "v" + UUIDTools::UUID.random_create.to_s.gsub(/-/, "")
    @service.admin_username = "au" + @node.generate_credential
    @service.admin_password = "ap" + @node.generate_credential
    @service.memory = @options[:memory]
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

		it "should start rabbit server with correct options" do
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

    it "should access the service instance using the credentials returned by sucessful provision" do
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

    it "should not allow null credentials to access the service instance" do
		  EM.run do
        AMQP.start(:host => @credentials["hostname"],
                   :vhost => @credentials["vhost"],) do |conn|
          conn.connected?.should == false
        end
        AMQP.stop
        EM.add_timer(1) {EM.stop}
      end
    end

    it "should not allow wrong credentials to access the service instance" do
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

    it "should not access the service instance when doing unprovision" do
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
      @service_credentials = @node.provision(:free)
      sleep 1
      @binding_credentials = @node.bind(@service_credentials["name"])
      sleep 1
    end

    after :all do
      @node.unbind(@binding_credentials)
      sleep 1
      @node.unprovision(@service_credentials["name"])
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

    it "should not allow null credentials to access the service instance" do
		  EM.run do
        AMQP.start(:host => @binding_credentials["hostname"],
                   :vhost => @binding_credentials["vhost"],) do |conn|
          conn.connected?.should == false
        end
        AMQP.stop
        EM.add_timer(1) {EM.stop}
      end
    end

    it "should not allow wrong credentials to access the service instance" do
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
      @service_credentials = @node.provision(:free)
      sleep 1
      @binding_credentials = @node.bind(@service_credentials["name"])
      sleep 1
      @response = @node.unbind(@binding_credentials)
      sleep 1
      @node.unprovision(@service_credentials["name"])
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

  describe "Node.save_service" do
    it "shuold raise error when save service instance failed" do
      @service.persisted_state=DataMapper::Resource::State::Immutable
      begin
        @node.save_service(@service)
      rescue => e
        e.class.should == VCAP::Services::Rabbit::RabbitError
      end
    end
  end

  describe "Node.destory_service" do
    it "shuold raise error when destroy service instance failed" do
      begin
        service = VCAP::Services::Rabbit::Node::ProvisionedService.new
        @node.destroy_service(service)
      rescue => e
        e.class.should == VCAP::Services::Rabbit::RabbitError
      end
    end
  end

  describe "Node.get_service" do
    it "shuold raise error when get service instance failed" do
      begin
        @node.get_service("non-existed")
      rescue => e
        e.class.should == VCAP::Services::Rabbit::RabbitError
      end
    end
  end

end
