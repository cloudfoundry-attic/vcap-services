# Copyright (c) 2009-2011 VMware, Inc.
require "redis"
require File.dirname(__FILE__) + '/spec_helper'
require "redis_service/redis_node"
require "redis_service/redis_error"

module VCAP
  module Services
    module Redis
      class Node
         attr_reader :base_dir, :redis_server_path, :local_ip, :available_memory, :max_memory, :max_swap, :node_id, :config_template, :free_ports
         attr_accessor :logger, :local_db
      end
    end
  end
end

describe VCAP::Services::Redis::Node do

  before :all do
    @logger = Logger.new(STDOUT, "daily")
    @logger.level = Logger::DEBUG
    @local_db_file = "/tmp/redis_node_" + Time.now.to_i.to_s + ".db"
    @options = {
      :logger => @logger,
      :base_dir => "/var/vcap/services/redis/instances",
      :redis_server_path => "redis-server",
      :local_ip => "127.0.0.1",
      :available_memory => 4096,
      :max_memory => 16,
      :max_swap => 32,
      :node_id => "redis-node-1",
      :config_template => File.expand_path("../resources/redis.conf.erb", File.dirname(__FILE__)),
      :local_db => "sqlite3:" + @local_db_file,
      :port_range => Range.new(5000, 25000),
      :mbus => "nats://localhost:4222",
    }

    # Start NATS server
    @uri = URI.parse(@options[:mbus])
    @pid_file = "/tmp/nats-redis-test.pid"
    if !NATS.server_running?(@uri)
      %x[ruby -S bundle exec nats-server -p #{@uri.port} -P #{@pid_file} -d 2> /dev/null]
    end
    sleep 1

    EM.run do
      @node = VCAP::Services::Redis::Node.new(@options)
      EM.add_timer(0.1) {EM.stop}
    end
  end

  before :each do
    @instance          = VCAP::Services::Redis::Node::ProvisionedInstance.new
    @instance.name     = "redis-#{UUIDTools::UUID.random_create.to_s}"
    @instance.port     = VCAP.grab_ephemeral_port
    @instance.plan     = :free
    @instance.password = UUIDTools::UUID.random_create.to_s
    @instance.memory   = @options[:max_memory]
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

  describe 'Node.initialize' do
    it "should set up a base directory" do
      @node.base_dir.should be @options[:base_dir]
    end

    it "should set up a redis server path" do
      @node.redis_server_path.should be @options[:redis_server_path]
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

    it "should set up a max swap size " do
      @node.max_swap.should be @options[:max_swap]
    end

    it "should load the redis configuration template" do
      @node.config_template.should be
    end

    it "should setup a free port set" do
      @node.free_ports.should be
    end
  end

  describe "Node.start_db" do
    it "should fail when set local db with non-existed file argument" do
      @node.local_db = "sqlite3:/non_existed/non-existed.db"
      begin
        @node.start_db
      rescue => e
        e.should be
      end
      @node.local_db = @options[:local_db]
    end

    it "should setup local db with right arguments" do
      @node.start_db.should be
    end
  end

  describe 'Node.start_provisioned_instances' do
    it "should check whether provisioned instance is running or not" do
      @instance.pid = @node.start_instance(@instance)
      sleep 1
      @instance.running?.should == true
      @node.stop_instance(@instance)
      sleep 1
      @instance.running?.should == false
    end

    it "should not start a new instance if the instance is already started when start all provisioned instances" do
      @instance.pid = @node.start_instance(@instance)
      @instance.save
      sleep 1
      @node.start_provisioned_instances
      instance = VCAP::Services::Redis::Node::ProvisionedInstance.get(@instance.name)
      instance.pid.should == @instance.pid
      @node.stop_instance(@instance)
      @instance.destroy
    end

    it "should start a new instance if the instance is not started when start all provisioned instances" do
      @instance.pid = @node.start_instance(@instance)
      @instance.save
      @node.stop_instance(@instance)
      sleep 1
      @node.start_provisioned_instances
      instance = VCAP::Services::Redis::Node::ProvisionedInstance.get(@instance.name)
      instance.pid.should_not == @instance.pid
      @node.stop_instance(@instance)
      @instance.destroy
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
      redis = Redis.new({:port => @credentials["port"], :password => @credentials["password"]})
      redis.get("test_key").should be_nil
    end

    it "should not allow null credentials to access the instance instance" do
      redis = Redis.new({:port => @credentials["port"]})
      begin
        redis.get("test_key")
      rescue => e
        e.class.should == RuntimeError
      end
    end

    it "should not allow wrong credentials to access the instance instance" do
      redis = Redis.new({:port => @credentials["port"], :password => "wrong_password"})
      begin
        redis.get("test_key")
      rescue => e
        e.class.should == RuntimeError
      end
    end

    it "should delete the provisioned instance port in free port list when finish a provision" do
      @node.free_ports.include?(@credentials["port"]).should == false
    end

    it "should decrease available memory when finish a provision" do
      (@old_memory - @node.available_memory).should == @node.max_memory
    end

    it "should send provision messsage when finish a provision" do
      @credentials["hostname"].should be
      @credentials["port"].should be
      @credentials["password"].should be
      @credentials["name"].should be
    end

    it "should provision from specified credentials" do
      in_credentials = {}
      in_credentials["name"] = "redis-#{UUIDTools::UUID.random_create.to_s}"
      in_credentials["port"] = 22222
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
      @old_memory = @node.available_memory
      sleep 1
      @node.unprovision(@credentials["name"])
    end

    it "should not access the instance instance when doing unprovision" do
      redis = Redis.new({:port => @credentials["port"], :password => @credentials["password"]})
      begin
        redis.get("test_key")
      rescue => e
        e.class.should == Errno::ECONNREFUSED
      end
    end

    it "should add the provisioned instance port in free port list when finish an unprovision" do
      @node.free_ports.include?(@credentials["port"]).should == true
    end

    it "should increase available memory when finish an unprovision" do
      (@node.available_memory - @old_memory).should == @node.max_memory
    end

    it "should raise error when unprovision an non-existed name" do
      begin
        @node.unprovision("non-existed")
      rescue => e
        e.class.should == VCAP::Services::Redis::RedisError
      end
    end
  end

  describe "Node.save_instance" do
    it "shuold raise error when save instance instance failed" do
      @instance.pid = 100
      @instance.persisted_state = DataMapper::Resource::State::Immutable
      begin
        @node.save_instance(@instance)
      rescue => e
        e.class.should == VCAP::Services::Redis::RedisError
      end
    end
  end

  describe "Node.destory_instance" do
    it "shuold raise error when destroy instance instance failed" do
      begin
        instance = VCAP::Services::Redis::Node::ProvisionedInstance.new
        @node.destroy_instance(instance)
      rescue => e
        e.class.should == VCAP::Services::Redis::RedisError
      end
    end
  end

  describe "Node.get_instance" do
    it "shuold raise error when get instance instance failed" do
      begin
        @node.get_instance("non-existed")
      rescue => e
        e.class.should == VCAP::Services::Redis::RedisError
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
      redis = Redis.new({:port => @binding_credentials["port"], :password => @binding_credentials["password"]})
      redis.get("test_key").should be_nil
    end

    it "should not allow null credentials to access the instance instance" do
      redis = Redis.new({:port => @binding_credentials["port"]})
      begin
        redis.get("test_key")
      rescue => e
        e.class.should == RuntimeError
      end
    end

    it "should not allow wrong credentials to access the instance instance" do
      redis = Redis.new({:port => @binding_credentials["port"], :password => "wrong_password"})
      begin
        redis.get("test_key")
      rescue => e
        e.class.should == RuntimeError
      end
    end

    it "should send binding messsage when finish a binding" do
      @binding_credentials["hostname"].should be
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

  describe "Node.memory_for_instance" do
    it "should return memory size by the plan" do
      instance = VCAP::Services::Redis::Node::ProvisionedInstance.new
      instance.plan = :free
      @node.memory_for_instance(instance).should == 16
    end

    it "should raise error when give wrong plan name" do
      instance = VCAP::Services::Redis::Node::ProvisionedInstance.new
      instance.plan = :non_existed_plan
      begin
        @node.memory_for_instance(instance)
      rescue => e
        e.class.should == VCAP::Services::Redis::RedisError
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
      varz[:provisioned_instances][0][:port].should == @credentials["port"]
      varz[:provisioned_instances][0][:plan].should == :free
      @node.unprovision(@credentials["name"])
    end
  end

  describe "Node.migration" do
    before :all do
      @credentials = @node.provision(:free)
      sleep 1
      Redis.new({:port => @credentials["port"], :password => @credentials["password"]}).set("test_key", "test_value")
      @dump_dir = File.join("/tmp/migration/redis", @credentials["name"])
      @binding_credentials1 = @node.bind(@credentials["name"])
      @binding_credentials2 = @node.bind(@credentials["name"])
      @binding_credentials_list = []
      @binding_credentials_list << @binding_credentials1
      @binding_credentials_list << @binding_credentials2
    end

    after :all do
      sleep 1
      @node.unbind(@binding_credentials1)
      @node.unbind(@binding_credentials2)
      @node.unprovision(@credentials["name"])
      FileUtils.rm_rf(@dump_dir)
    end

    it "should not access redis server after disable the instance" do
      @node.disable_instance(@credentials, @binding_credentials_list)
      sleep 1
      begin
        @node.get_info(@credentials["port"], @credentials["password"])
      rescue => e
        e.class.should == VCAP::Services::Redis::RedisError
      end
    end

    it "should dump db file to right location after dump instance" do
      @node.dump_instance(@credentials, @binding_credentials_list, @dump_dir)
      dump_file = File.join(@dump_dir, "dump.rdb")
      File.exists?(dump_file).should == true
    end

    it "should access redis server in old node after enable the instance" do
      @node.enable_instance(@credentials, @binding_credentials_list)
      sleep 1
      @node.check_password(@credentials["port"], @credentials["password"]).should == true
    end

    it "should import db file from right location after import instance" do
      @node.unprovision(@credentials["name"])
      sleep 1
      @node.import_instance(@credentials, @binding_credentials_list, @dump_dir, :free)
      sleep 1
      credentials_list = @node.enable_instance(@credentials, @binding_credentials_list)
      credentials_list.size.should == 3
      credentials_list.each do |credentials|
        Redis.new({:port => credentials["port"], :password => credentials["password"]}).get("test_key").should == "test_value"
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
