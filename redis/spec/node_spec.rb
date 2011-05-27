# Copyright (c) 2009-2011 VMware, Inc.
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
    @logger.level = Logger::ERROR
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

    it "should access the instance using the credentials returned by successful provision" do
      redis = Redis.new({:port => @credentials["port"], :password => @credentials["password"]})
      redis.get("test_key").should be_nil
    end

    it "should not allow null credentials to access the instance" do
      redis = Redis.new({:port => @credentials["port"]})
      expect {redis.get("test_key")}.should raise_error(RuntimeError)
    end

    it "should not allow wrong credentials to access the instance" do
      redis = Redis.new({:port => @credentials["port"], :password => "wrong_password"})
      expect {redis.get("test_key")}.should raise_error(RuntimeError)
    end

    it "should delete the provisioned instance port in free port list when finish a provision" do
      @node.free_ports.include?(@credentials["port"]).should == false
    end

    it "should decrease available memory when finish a provision" do
      (@old_memory - @node.available_memory).should == @node.max_memory
    end

    it "should send provision message when finish a provision" do
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

    it "should not access the instance when doing unprovision" do
      redis = Redis.new({:port => @credentials["port"], :password => @credentials["password"]})
      expect {redis.get("test_key")}.should raise_error(Errno::ECONNREFUSED)
    end

    it "should add the provisioned instance port in free port list when finish an unprovision" do
      @node.free_ports.include?(@credentials["port"]).should == true
    end

    it "should increase available memory when finish an unprovision" do
      (@node.available_memory - @old_memory).should == @node.max_memory
    end

    it "should raise exception when unprovision an non-existed name" do
      expect {@node.unprovision("non-existed")}.should raise_error(VCAP::Services::Redis::RedisError)
    end
  end

  describe "Node.save_instance" do
    it "should raise exception when save instance failed" do
      @instance.pid = 100
      @instance.persisted_state = DataMapper::Resource::State::Immutable
      expect {@node.save_instance(@instance)}.should raise_error(VCAP::Services::Redis::RedisError)
    end
  end

  describe "Node.destory_instance" do
    it "should raise exception when destroy instance failed" do
      instance = VCAP::Services::Redis::Node::ProvisionedInstance.new
      expect {@node.destroy_instance(instance)}.should raise_error(VCAP::Services::Redis::RedisError)
    end
  end

  describe "Node.get_instance" do
    it "should raise exception when get instance failed" do
      expect {@node.get_instance("non-existed")}.should raise_error(VCAP::Services::Redis::RedisError)
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

    it "should access redis server using the returned credential" do
      redis = Redis.new({:port => @binding_credentials["port"], :password => @binding_credentials["password"]})
      redis.get("test_key").should be_nil
    end

    it "should not allow null credentials to access the instance" do
      redis = Redis.new({:port => @binding_credentials["port"]})
      expect {redis.get("test_key")}.should raise_error(RuntimeError)
    end

    it "should not allow wrong credentials to access the instance" do
      redis = Redis.new({:port => @binding_credentials["port"], :password => "wrong_password"})
      expect {redis.get("test_key")}.should raise_error(RuntimeError)
    end

    it "should send binding message when finish a binding" do
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

    it "should raise exception when giving wrong plan name" do
      instance = VCAP::Services::Redis::Node::ProvisionedInstance.new
      instance.plan = :non_existed_plan
      expect {@node.memory_for_instance(instance)}.should raise_error(VCAP::Services::Redis::RedisError)
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

  describe "Node.restore" do
    before :all do
      @restore_dir = "/tmp/restore/redis"
      FileUtils.mkdir_p(@restore_dir)
      @credentials1 = @node.provision(:free)
      @credentials2 = @node.provision(:free)
      sleep 1
      Redis.new({:port => @credentials1["port"], :password => @credentials1["password"]}).set("test_key", "test_value")
      @node.set_config(@credentials1["port"], @credentials1["password"], "save", "1 0")
      @node.set_config(@credentials1["port"], @credentials1["password"], "dir", @restore_dir)
      sleep 2
      @restore_result = @node.restore(@credentials2["name"], @restore_dir)
      sleep 1
    end

    after :all do
      @node.unprovision(@credentials1["name"])
      @node.unprovision(@credentials2["name"])
      FileUtils.rm_rf("/tmp/restore")
    end

    it "should restore user data from a backup file" do
      Redis.new({:port => @credentials2["port"], :password => @credentials2["password"]}).get("test_key").should == "test_value"
    end

    it "should return an empty hash if restore successfully" do
      @restore_result.should == {}
    end

    it "should raise exception if the instance is not existed" do
      expect {@node.restore("non-existed", @restore_dir)}.should raise_error(VCAP::Services::Redis::RedisError)
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
      @node.unbind(@binding_credentials1)
      @node.unbind(@binding_credentials2)
      @node.unprovision(@credentials["name"])
      FileUtils.rm_rf(@dump_dir)
    end

    it "should not access redis server after disable the instance" do
      @node.disable_instance(@credentials, @binding_credentials_list)
      sleep 1
      expect {@node.get_info(@credentials["port"], @credentials["password"])}.should raise_error(VCAP::Services::Redis::RedisError)
    end

    it "should dump db file to right location after dump instance" do
      @node.dump_instance(@credentials, @binding_credentials_list, @dump_dir)
      dump_file = File.join(@dump_dir, "dump.rdb")
      File.exists?(dump_file).should == true
    end

    it "should access redis server in old node after enable the instance" do
      @node.enable_instance(@credentials, @binding_credentials_map)
      sleep 1
      @node.check_password(@credentials["port"], @credentials["password"]).should == true
    end

    it "should import db file from right location after import instance" do
      @node.unprovision(@credentials["name"])
      sleep 1
      @node.import_instance(@credentials, @binding_credentials_list, @dump_dir, :free)
      sleep 1
      credentials_list = @node.enable_instance(@credentials, @binding_credentials_map)
      credentials_list.size.should == 2
      Redis.new({:port => credentials_list[0]["port"], :password => credentials_list[0]["password"]}).get("test_key").should == "test_value"
      credentials_list[1].each do |key, value|
        Redis.new({:port => value["credentials"]["port"], :password => value["credentials"]["password"]}).get("test_key").should == "test_value"
      end
    end
  end

  describe "Node.restart" do
    it "should still use the provisioned service after the restart" do
      EM.run do
        credentials = @node.provision(:free)
        @node.shutdown
        sleep 1
        @node.start
        sleep 2
        @node.check_password(credentials["port"], credentials["password"]).should == true
        @node.unprovision(credentials["name"])
        EM.add_timer(0.1) {EM.stop}
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
