# Copyright (c) 2009-2011 VMware, Inc.
require File.dirname(__FILE__) + "/spec_helper"

module VCAP
  module Services
    module Redis
      class Node
         attr_reader :base_dir, :redis_server_path, :local_ip, :capacity, :max_memory, :max_swap, :node_id, :config_template, :free_ports, :redis_timeout
         attr_accessor :logger, :local_db
      end
    end
  end
end

describe VCAP::Services::Redis::Node do

  before :all do
    @options = getNodeTestConfig
    @options.freeze
    FileUtils.mkdir_p(@options[:base_dir])
    FileUtils.mkdir_p(@options[:redis_log_dir])

    # Setup code must be wrapped in EM.run
    EM.run do
      @node = VCAP::Services::Redis::Node.new(@options)
      EM.stop
    end
  end

  before :each do
    @instance          = VCAP::Services::Redis::Node::ProvisionedService.new
    @instance.name     = UUIDTools::UUID.random_create.to_s
    @instance.port     = VCAP.grab_ephemeral_port
    @instance.plan     = 1
    @instance.password = UUIDTools::UUID.random_create.to_s
    @instance.memory   = @options[:max_memory]
  end

  after :all do
    FileUtils.rm_f(@options[:local_db_file])
    FileUtils.rm_rf(@options[:base_dir])
    FileUtils.rm_rf(@options[:redis_log_dir])
  end

  describe "Node.initialize" do
    it "should set up a base directory" do
      @node.base_dir.should be @options[:base_dir]
    end

    it "should set up a redis server path" do
      @node.redis_server_path.should be @options[:redis_server_path]
    end

    it "should set up a local IP" do
      @node.local_ip.should be
    end

    it "should set up an available capacity" do
      @node.capacity.should be
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

  describe "Node.start_provisioned_instances" do
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
      @instance.plan = 1
      @instance.save
      sleep 1
      @node.start_provisioned_instances
      sleep 1
      instance = VCAP::Services::Redis::Node::ProvisionedService.get(@instance.name)
      instance.pid.should == @instance.pid
      @node.stop_instance(@instance)
      @instance.destroy
    end

    it "should start a new instance if the instance is not started when start all provisioned instances" do
      @instance.pid = @node.start_instance(@instance)
      @instance.plan = 1
      @instance.save
      @node.stop_instance(@instance)
      sleep 1
      @node.start_provisioned_instances
      sleep 1
      instance = VCAP::Services::Redis::Node::ProvisionedService.get(@instance.name)
      instance.pid.should_not == @instance.pid
      @node.stop_instance(@instance)
      @instance.destroy
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
      @old_capacity = @node.capacity
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
      @old_capacity = @node.capacity
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
      instance = VCAP::Services::Redis::Node::ProvisionedService.new
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

  describe "Node.memory_for_instance" do
    it "should return memory size by the plan" do
      instance = VCAP::Services::Redis::Node::ProvisionedService.new
      instance.plan = 1
      @node.memory_for_instance(instance).should == @node.max_memory
    end
  end

  describe "Node.varz_details" do
    it "should report varz details" do
      @credentials = @node.provision(:free)
      sleep 1
      varz = @node.varz_details
      varz[:provisioned_instances_num].should == 1
      varz[:provisioned_instances][0][:name].should == @credentials["name"]
      varz[:provisioned_instances][0][:port].should == @credentials["port"]
      varz[:provisioned_instances][0][:plan].should == "free"
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

    it "should restore from an empty dump file" do
      restore_dir = "/tmp/restore/redis_empty"
      FileUtils.mkdir_p(restore_dir)
      FileUtils.touch(File.join(restore_dir, "dump.rdb"))
      restore_result = @node.restore(@credentials2["name"], restore_dir)
      sleep 1
      Redis.new({:port => @credentials2["port"], :password => @credentials2["password"]}).get("test_key").should be_nil
    end

    it "should raise exception if the instance is not existed" do
      expect {@node.restore("non-existed", @restore_dir)}.should raise_error(VCAP::Services::Redis::RedisError)
    end

    it "should raise exception if the dumped file is not existed" do
      expect {@node.restore(@credentials2["name"], "/tmp/restore")}.should raise_error(VCAP::Services::Redis::RedisError)
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
      FileUtils.rm_rf("/tmp/migration")
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
      @node.import_instance(@credentials, @binding_credentials_map, @dump_dir, :free)
      sleep 1
      credentials_list = @node.enable_instance(@credentials, @binding_credentials_map)
      credentials_list.size.should == 2
      Redis.new({:port => credentials_list[0]["port"], :password => credentials_list[0]["password"]}).get("test_key").should == "test_value"
      credentials_list[1].each do |key, value|
        Redis.new({:port => value["credentials"]["port"], :password => value["credentials"]["password"]}).get("test_key").should == "test_value"
      end
    end
  end

  describe "Node.max_clients" do
    it "should limit the maximum number of clients" do
      @credentials = @node.provision(:free)
      sleep 1
      redis = []
      # Create max_clients connections
      for i in 1..@options[:max_clients]
        redis[i] = Redis.new({:port => @credentials["port"], :password => @credentials["password"]})
        redis[i].info
      end
      # The max_clients + 1 connection will raise exception
      expect {Redis.new({:port => @credentials["port"], :password => @credentials["password"]}).info}.should raise_error(RuntimeError)
      # Close the max_clients connections
      for i in 1..@options[:max_clients]
        redis[i].quit
      end
      # Now the new connection will be successful
      Redis.new({:port => @credentials["port"], :password => @credentials["password"]}).info
      @node.unprovision(@credentials["name"])
    end
  end

  describe "Node.timeout" do
    it "should raise exception when redis client response time is too long" do
      credentials = @node.provision(:free)
      sleep 1
      class Redis
        alias :old_info :info
        def info(cmd = nil)
          sleep 3
          old_info(cmd)
        end
      end
      expect {@node.get_info(credentials["port"], credentials["password"])}.should raise_error(VCAP::Services::Redis::RedisError)
      class Redis
        alias :info :old_info
      end
      @node.get_info(credentials["port"], credentials["password"]).should be
      @node.unprovision(credentials["name"])
    end
  end

  describe "Node.orphan" do
    it "should return proper instance list" do
      before_instances = @node.all_instances_list
      oi = @node.provision(:free)
      after_instances = @node.all_instances_list
      @node.unprovision(oi["name"])
      (after_instances - before_instances).include?(oi["name"]).should be_true
    end
  end

  describe "Node.thread_safe" do
    it "should be thread safe in multi-threads call" do
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
      new_ports = @node.free_ports.clone
      delta_ports = Set.new
      credentials_list.each do |credentials|
        delta_ports << credentials["port"]
      end
      (old_ports - new_ports).should == delta_ports
      VCAP::Services::Redis::Node::ProvisionedService.all.size.should == threads_num
      somethreads = (1..threads_num).collect do |i|
        Thread.new do
          @node.unprovision(credentials_list[i - 1]["name"])
        end
      end
      somethreads.each {|t| t.join}
      @node.free_ports.should == old_ports
      VCAP::Services::Redis::Node::ProvisionedService.all.size.should == 0
    end
  end

  describe "Node.restart" do
    it "should still use the provisioned service after the restart" do
      EM.run do
        credentials = @node.provision(:free)
        @node.shutdown
        @node = VCAP::Services::Redis::Node.new(@options)
        EM.add_timer(1) {
          @node.check_password(credentials["port"], credentials["password"]).should == true
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
        EM.add_timer(0.1) {EM.stop}
      end
    end
  end

end
