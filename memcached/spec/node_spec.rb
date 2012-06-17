# Copyright (c) 2009-2011 VMware, Inc.
# This code is based on Redis as a Service.

require "spec_helper"
require "memcached_service/memcached_node"
require "memcached_service/memcached_error"
require "dalli"

module VCAP
  module Services
    module Memcached
      class Node
         attr_reader :base_dir, :memcached_server_path, :local_ip, :capacity, :node_id, :config_template, :free_ports, :memcached_timeout
         attr_accessor :logger, :local_db
      end
    end
  end
end

describe VCAP::Services::Memcached::Node do

  before :all do
    @capacity_unit = 1
    @options = get_node_config()
    @logger = @options[:logger]
    @local_db_file = @options[:local_db_file]
    FileUtils.mkdir_p(@options[:base_dir])
    FileUtils.mkdir_p(@options[:memcached_log_dir])

    EM.run do
      @node = VCAP::Services::Memcached::Node.new(@options)
      EM.add_timer(0.1) {EM.stop}
    end
  end

  before :each do
    @instance          = VCAP::Services::Memcached::Node::ProvisionedService.new
    @instance.name     = UUIDTools::UUID.random_create.to_s
    @instance.user     = UUIDTools::UUID.random_create.to_s
    @instance.port     = VCAP.grab_ephemeral_port
    @instance.plan     = :free
    @instance.password = UUIDTools::UUID.random_create.to_s
  end

  after :all do
    FileUtils.rm_f(@local_db_file)
    FileUtils.rm_rf(@options[:base_dir])
    FileUtils.rm_rf(@options[:memcached_log_dir])
  end

#  describe 'SASLAdmin' do
#    before :all do
#      @admin = VCAP::Services::Memcached::Node::SASLAdmin.new(@logger)
#      @create_user = 'username'
#      @password = 'password'
#    end
#
#    it "should create new user" do
#      @admin.create_user(@create_user, @password)
#    end
#
#    it "should delete specified user" do
#      @admin.delete_user(@create_user)
#    end
#  end

  describe 'Node.initialize' do
    it "should set up a base directory" do
      @node.base_dir.should be @options[:base_dir]
    end

    it "should set up a memcached server path" do
      @node.memcached_server_path.should be @options[:memcached_server_path]
    end

    it "should set up a local IP" do
      @node.local_ip.should be
    end

    it "should set up an available capacity" do
      @node.capacity.should == @options[:capacity]
    end

    it "should setup a free port set" do
      @node.free_ports.should be
    end
  end

  describe "Node.start_db" do
    it "should fail when set local db with non-existing file argument" do
      @node.local_db = "sqlite3:/non_existing/non-existing.db"
      thrown = nil
      begin
        @node.start_db
      rescue => e
        thrown = e
      end
      thrown.should be
      thrown.class.should == DataObjects::ConnectionError
      @node.local_db = @options[:local_db]
    end

    it "should setup local db with right arguments" do
      @node.start_db.should be
    end
  end

  describe 'Node.start_provisioned_instances' do
    it "should check whether provisioned instance is running or not" do
      @logger.debug("test : start instance #{@instance.inspect}")
      @instance.pid = @node.start_instance(@instance)
      sleep 2
      @instance.running?.should == true
      @logger.debug("test : stop instance #{@instance.inspect}")
      @node.stop_instance(@instance)
      sleep 2
      @instance.running?.should == false
    end

    it "should start a new instance if the instance is not started when start all provisioned instances" do
      @instance.pid = @node.start_instance(@instance)
      @node.save_instance(@instance)
      @node.stop_instance(@instance)
      sleep 1
      @node.start_provisioned_instances
      sleep 1
      instance = VCAP::Services::Memcached::Node::ProvisionedService.get(@instance.name)
      p instance
      instance.pid.should_not == @instance.pid
      @node.stop_instance(@instance)
      @instance.destroy
    end
  end

  describe 'Node.announcement' do
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
      sleep 1
    end

    after :all do
      @node.unprovision(@credentials["name"])
    end

    it "should access the instance using the credentials returned by successful provision" do
      hostname, username, password = get_connect_info(@credentials)
      memcached = Dalli::Client.new(hostname, username: username, password: password)
      memcached.get("test_key").should be_nil
    end

# - These tests require sasl to be enabled, commenting them out for now if later on we decide
#   to enable sasl
#    it "should not allow null credentials to access the instance" do
#      hostname = get_hostname(@credentials)
#      memcached = Dalli::Client.new(hostname)
#      expect {memcached.get("test_key")}.should raise_error(RuntimeError)
#    end

#    it "should not allow wrong credentials to access the instance" do
#      hostname, username, password = get_connect_info(@credentials)
#      memcached = Dalli::Client.new(hostname, username: username, password: 'wrong_password')
#      expect {memcached.get("test_key")}.should raise_error(RuntimeError)
#    end

    it "should delete the provisioned instance port in free port list when finish a provision" do
      @node.free_ports.include?(@credentials["port"]).should == false
    end

    it "should send provision message when finish a provision" do
      @credentials["hostname"].should be
      @credentials["host"].should == @credentials["hostname"]
      @credentials["port"].should be
      @credentials["password"].should be
      @credentials["name"].should be
    end

    it "should provision from specified credentials" do
      in_credentials = {}
      in_credentials["name"] = UUIDTools::UUID.random_create.to_s
      in_credentials["port"] = 22222
      in_credentials["user"] = UUIDTools::UUID.random_create.to_s
      in_credentials["password"] = UUIDTools::UUID.random_create.to_s
      out_credentials = @node.provision(:free, in_credentials)
      sleep 1
      out_credentials["name"].should == in_credentials["name"]
      out_credentials["port"].should == in_credentials["port"]
      out_credentials["password"].should == in_credentials["password"]
      @node.unprovision(out_credentials["name"])
    end
  end

  describe "Node.unprovision" do
    before :all do
      @credentials = @node.provision(:free)
      sleep 2
      @node.unprovision(@credentials["name"])
    end

    it "should not access the instance when doing unprovision" do
      p @credentials
      hostname, username, password = get_connect_info(@credentials)
      memcached = Dalli::Client.new(hostname, username: username, password: password)
      expect {memcached.get("test_key")}.should raise_error(Dalli::DalliError)
    end

    it "should add the provisioned instance port in free port list when finish an unprovision" do
      @node.free_ports.include?(@credentials["port"]).should == true
    end

    it "should raise exception when unprovision an non-existed name" do
      expect {@node.unprovision("non-existed")}.should raise_error(VCAP::Services::Memcached::MemcachedError)
    end
  end

  describe "Node.save_instance" do
    it "should raise exception when save instance failed" do
      @instance.pid = 100
      @instance.persisted_state = DataMapper::Resource::State::Immutable
      expect {@node.save_instance(@instance)}.should raise_error(VCAP::Services::Memcached::MemcachedError)
    end
  end

  describe "Node.destory_instance" do
    it "should raise exception when destroy instance failed" do
      instance = VCAP::Services::Memcached::Node::ProvisionedService.new
      expect {@node.destroy_instance(instance)}.should raise_error(VCAP::Services::Memcached::MemcachedError)
    end
  end

  describe "Node.get_instance" do
    it "should raise exception when get instance failed" do
      expect {@node.get_instance("non-existed")}.should raise_error(VCAP::Services::Memcached::MemcachedError)
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

    it "should access memcached server using the returned credential" do
      hostname = get_hostname(@binding_credentials)
      username = @binding_credentials['user']
      password = @binding_credentials['password']
      memcached = Dalli::Client.new(hostname, username: username, password: password)
      memcached.get("test_key").should be_nil
    end

# - These tests require sasl to be enabled, commenting them out for now if later on we decide
#   to enable sasl
#    it "should not allow null credentials to access the instance" do
#      hostname = get_hostname(@binding_credentials)
#      memcached = Dalli::Client.new(hostname)
#      expect {memcached.get("test_key")}.should raise_error(RuntimeError)
#    end

#    it "should not allow wrong credentials to access the instance" do
#      hostname = get_hostname(@binding_credentials)
#      username = @binding_credentials['user']
#      password = @binding_credentials['password']
#      memcached = Dalli::Client.new(hostname, username: username, password: 'wrong_password')
#      expect {memcached.get("test_key")}.should raise_error(RuntimeError)
#    end

    it "should send binding message when finish a binding" do
      @binding_credentials["hostname"].should be
      @binding_credentials["host"].should == @binding_credentials["hostname"]
      @binding_credentials["port"].should be
      @binding_credentials["password"].should be
      @binding_credentials["name"].should be
    end
  end

  describe "Node.unbind" do
    it "should return true when finish an unbinding" do
      @instance_credentials = @node.provision(:free)
      sleep 1
      @binding_credentials = @node.bind(@instance_credentials["name"])
      sleep 1
      @node.unbind(@binding_credentials).should == {}
      @node.unprovision(@instance_credentials["name"])
    end
  end

  describe "Node.varz_details" do
    it "should report varz details" do
      @credentials = @node.provision(:free)
      sleep 1
      varz = @node.varz_details
      varz[:provisioned_instances_num].should == 1
      varz[:max_instances_num].should == @options[:capacity] / @capacity_unit
      varz[:provisioned_instances][0][:name].should == @credentials["name"]
      varz[:provisioned_instances][0][:port].should == @credentials["port"]
      varz[:provisioned_instances][0][:plan].should == :free
      @node.unprovision(@credentials["name"])
    end
  end

  describe "Node.max_clients" do
    it "should limit the maximum number of clients" do
      @credentials = @node.provision(:free)
      sleep 1
      memcached = []
      # Create max_clients connections
      hostname, username, password = get_connect_info(@credentials)
      for i in 1..(@options[:max_clients]-30)
        memcached[i] = Dalli::Client.new(hostname, username: username, password: password)
        memcached[i].set("foo", 1)
      end

      # The max_clients + 1 connection will raise exception
      expect do
        Dalli::Client.new(hostname, username: username, password: password).set("foo", 1)
      end.should raise_error(Dalli::RingError)
      # Close the max_clients connections
      for i in 1..(@options[:max_clients] - 30)
        memcached[i].close
      end
      # Now the new connection will be successful
      new_memcached = Dalli::Client.new(hostname, username: username, password: password)
      new_memcached.set('foo', 1)
      @node.unprovision(@credentials["name"])
    end
  end

  describe "Node.timeout" do
    it "should raise exception when memcached client response time is too long" do
      credentials = @node.provision(:free)
      instance = @node.get_instance(credentials["name"])
      sleep 1
      class Dalli::Client
        alias :old_stats :stats
        def stats(cmd = nil)
          sleep 3
          old_stats(cmd)
        end
      end
      expect {@node.get_info(instance)}.should raise_error(VCAP::Services::Memcached::MemcachedError)
      class Dalli::Client
        alias :stats :old_stats
      end
      @node.get_info(instance).should be
      @node.unprovision(credentials["name"])
    end
  end

  # TODO: This test should be ideally for the base class...
=begin
  describe "Node.thread_safe" do
    it "should be thread safe in multi-threads call" do
      old_capacity = @node.available_capacity
      old_ports = @node.free_ports.clone
      semaphore = Mutex.new
      credentials_list = []
      threads_num = 10
      somethreads = (1..threads_num).collect do
        Thread.new do
          semaphore.synchronize do
            credentials_list << @node.provision(:free)
          end
        end
      end
      somethreads.each {|t| t.join}
      sleep 2
      new_capacity = @node.available_capacity
      new_ports = @node.free_ports.clone
      (old_capacity - new_capacity).should == threads_num * @capacity_unit
      delta_ports = Set.new
      credentials_list.each do |credentials|
        delta_ports << credentials["port"]
      end
      (old_ports - new_ports).should == delta_ports
      VCAP::Services::Memcached::Node::ProvisionedService.all.size.should == threads_num
      somethreads = (1..threads_num).collect do |i|
        Thread.new do
          @node.unprovision(credentials_list[i - 1]["name"])
        end
      end
      somethreads.each {|t| t.join}
      @node.free_ports.should == old_ports
      @node.available_capacity.should == old_capacity
      VCAP::Services::Memcached::Node::ProvisionedService.all.size.should == 0
    end
  end
=end

  describe "Node.restart" do
    it "should still use the provisioned service after the restart" do
      begin
        EM.run do
          credentials = @node.provision(:free)
          @node.shutdown
          @node = VCAP::Services::Memcached::Node.new(@options)
          @node.get_instance(credentials).should_not == nil
          EM.add_timer(1) {
            memcached = Dalli::Client.new(hostname, username: username, password: password)
            memcached.stats

            @node.unprovision(credentials["name"])
            EM.stop
          }
        end
      rescue SystemExit => err
      end
    end
  end

  describe "Node.shutdown" do
    it "should return true when shutdown finished" do
      EM.run do
        @node.shutdown.should be
        EM.add_timer(0.1) {EM.stop}
      end
    end
  end

end
