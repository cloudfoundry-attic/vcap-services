# Copyright (c) 2009-2011 VMware, Inc.
require File.dirname(__FILE__) + "/spec_helper"

module VCAP
  module Services
    module Redis
      class Node
        attr_reader :options, :free_ports, :capacity, :config_command_name, :shutdown_command_name, :save_command_name, :disable_password
        attr_accessor :logger, :local_db
      end
    end
  end
end

describe VCAP::Services::Redis::Node do

  before :all do
    @options = getNodeTestConfig
    FileUtils.mkdir_p(@options[:base_dir])
    FileUtils.mkdir_p(@options[:image_dir])
    FileUtils.mkdir_p(@options[:redis_log_dir])
    FileUtils.mkdir_p(@options[:migration_nfs])

    # Setup code must be wrapped in EM.run
    EM.run do
      @node = VCAP::Services::Redis::Node.new(@options)
      EM.add_timer(1) {EM.stop}
    end
    @redis_port = @node.options[:instance_port]
  end

  after :all do
    FileUtils.rm_f(@options[:local_db_file])
    FileUtils.rm_rf(@options[:base_dir])
    FileUtils.rm_rf(@options[:redis_log_dir])
    FileUtils.rm_rf(@options[:image_dir])
    FileUtils.rm_rf(@options[:migration_nfs])
  end

  describe "Node.announcement" do
    it "should send node announcement" do
      @node.announcement.should be
    end

    it "should send available_capacity in announce message" do
      @node.announcement[:available_capacity].should == @node.capacity
    end
  end

  describe "Node.pre_send_announcement" do
    before :all do
      @old_capacity = @node.capacity
      @credentials = @node.provision(:free)
      @node.shutdown
      EM.run do
        @node = VCAP::Services::Redis::Node.new(@options)
        EM.add_timer(1) {EM.stop}
      end
    end

    after :all do
      @node.unprovision(@credentials["name"])
    end

    it "should start provisioned instances before sending announcement" do
      instance = @node.get_instance(@credentials["name"])
      redis_echo(instance.ip, @redis_port, @credentials["password"]).should == true
    end

    it "should decrease the capacity" do
      @node.capacity.should == @old_capacity - 1
    end
  end

  describe "Node.provision" do
    before :all do
      @credentials = @node.provision(:free)
      @instance = @node.get_instance(@credentials["name"])
    end

    after :all do
      @node.unprovision(@credentials["name"])
    end

    it "should access the instance using the credentials returned by successful provision" do
      redis_echo(@instance.ip, @redis_port, @credentials["password"]).should == true
    end

    it "should not allow null credentials to access the instance" do
      expect {redis_echo(@instance.ip, @redis_port)}.should raise_error(RuntimeError)
    end

    it "should not allow wrong credentials to access the instance" do
      expect {redis_echo(@instance.ip, @redis_port, "wrong_password")}.should raise_error(RuntimeError)
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
      out_credentials["name"].should == in_credentials["name"]
      out_credentials["port"].should == in_credentials["port"]
      out_credentials["password"].should == in_credentials["password"]
      @node.unprovision(out_credentials["name"])
    end
  end

  describe "Node.unprovision" do
    before :all do
      @credentials = @node.provision(:free)
      @instance = @node.get_instance(@credentials["name"])
      @node.unprovision(@credentials["name"])
    end

    it "should not access the instance after unprovision" do
      expect {redis_echo(@instance.ip, @redis_port, @credentials["password"])}.should raise_error
    end

    it "should add the provisioned instance port in free port set when finish an unprovision" do
      @node.free_ports.include?(@credentials["port"]).should == true
    end

    it "should raise exception when unprovision a non-existed instance name" do
      expect {@node.unprovision("non-existed")}.should raise_error(VCAP::Services::Base::Error::ServiceError)
    end
  end

  describe "Node.bind" do
    before :all do
      @instance_credentials = @node.provision(:free)
      @instance = @node.get_instance(@instance_credentials["name"])
      @binding_credentials = @node.bind(@instance_credentials["name"])
    end

    after :all do
      @node.unbind(@binding_credentials)
      @node.unprovision(@instance_credentials["name"])
    end

    it "should access redis server using the returned credentials" do
      redis_echo(@instance.ip, @redis_port, @binding_credentials["password"]).should == true
    end

    it "should not allow null credentials to access the instance" do
      expect {redis_echo(@instance.ip, @redis_port)}.should raise_error(RuntimeError)
    end

    it "should not allow wrong credentials to access the instance" do
      expect {redis_echo(@instance.ip, @redis_port, "wrong_password")}.should raise_error(RuntimeError)
    end

    it "should send binding message when finish binding" do
      @binding_credentials["hostname"].should be
      @binding_credentials["host"].should == @binding_credentials["hostname"]
      @binding_credentials["port"].should be
      @binding_credentials["password"].should be
      @binding_credentials["name"].should be
    end
  end

  describe "Node.unbind" do
    it "should return an empty hash when finish unbinding" do
      @instance_credentials = @node.provision(:free)
      @binding_credentials = @node.bind(@instance_credentials["name"])
      @node.unbind(@binding_credentials).should == {}
      @node.unprovision(@instance_credentials["name"])
    end
  end

  describe "Node.varz_details" do
    it "should report varz details" do
      @credentials = @node.provision(:free)
      varz = @node.varz_details
      varz[:provisioned_instances_num].should == 1
      varz[:provisioned_instances][0][:name].should == @credentials["name"]
      varz[:provisioned_instances][0][:port].should == @credentials["port"]
      varz[:provisioned_instances][0][:plan].should == 1
      @node.unprovision(@credentials["name"])
    end
  end

  describe "Node.restore" do
    before :all do
      @credentials = @node.provision(:free)
      @restore_result = @node.restore(@credentials["name"], "./")
      @instance = @node.get_instance(@credentials["name"])
    end

    after :all do
      @node.unprovision(@credentials["name"])
    end

    it "should restore user data from a backup file" do
      redis_get(@instance.ip, @redis_port, @credentials["password"], "test_key").should == "test_value"
    end

    it "should return an empty hash if restore successfully" do
      @restore_result.should == {}
    end

    it "should restore from an empty dump file" do
      restore_dir = "/tmp/redis_empty"
      FileUtils.mkdir_p(restore_dir)
      FileUtils.touch(File.join(restore_dir, "dump.rdb"))
      restore_result = @node.restore(@credentials["name"], restore_dir)
      restore_result.should == {}
      @instance = @node.get_instance(@credentials["name"])
      redis_get(@instance.ip, @redis_port, @credentials["password"], "test_key").should be_nil
      FileUtils.rm_rf(restore_dir)
    end

    it "should raise exception if the instance is not existed" do
      expect {@node.restore("non-existed", "/tmp")}.should raise_error(VCAP::Services::Redis::RedisError)
    end

    it "should raise exception if the dumped file is not existed" do
      expect {@node.restore(@credentials["name"], "/tmp/restore")}.should raise_error(VCAP::Services::Redis::RedisError)
    end
  end

  describe "Node.migration" do
    before :all do
      @credentials = @node.provision(:free)
      @instance = @node.get_instance(@credentials["name"])
      redis_set(@instance.ip, @redis_port, @credentials["password"], "test_key", "test_value")
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
      @node.unbind(@binding_credentials1)
      @node.unbind(@binding_credentials2)
      @node.unprovision(@credentials["name"])
    end

    it "should not access redis server after disable the instance" do
      @node.disable_instance(@credentials, @binding_credentials_list).should == true
      expect {redis_echo(@instance.ip, @redis_port, @credentials["password"])}.should raise_error(RuntimeError)
    end

    it "should dump db file to right location after dump instance" do
      begin
        @node.dump_instance(@credentials, @binding_credentials_list, @node.options[:migration_nfs]).should == true
        dump_file = File.join(@node.options[:migration_nfs], "dump.rdb")
        File.exists?(dump_file).should == true
      rescue => e
        p e
      end
    end

    it "should access redis server in old node after enable the instance" do
      @node.enable_instance(@credentials, @binding_credentials_map).should == true
      redis_echo(@instance.ip, @redis_port, @credentials["password"]).should == true
    end

    it "should import db file from right location after import instance" do
      @node.unprovision(@credentials["name"])
      @node.import_instance(@credentials, @binding_credentials_map, "./", :free)
      credentials_list = @node.update_instance(@credentials, @binding_credentials_map)
      credentials_list.size.should == 2
      instance = @node.get_instance(credentials_list[0]["name"])
      redis_get(instance.ip, @redis_port, credentials_list[0]["password"], "test_key").should == "test_value"
      credentials_list[1].each do |_, value|
        redis_get(instance.ip, @redis_port, value["credentials"]["password"], "test_key").should == "test_value"
      end
    end
  end

  describe "Node.max_clients" do
    it "should limit the maximum number of clients" do
      credentials = @node.provision(:free)
      instance = @node.get_instance(credentials["name"])
      begin
        redis = []
        # Create max_clients connections
        for i in 1..@options[:max_clients]
          redis[i] = Redis.new({:host => instance.ip, :port => @redis_port, :password => credentials["password"]})
          redis[i].info
        end
        # The max_clients + 1 connection will raise exception
        expect {Redis.new({:host => instance.ip, :port => @redis_port, :password => credentials["password"]}).info}.should raise_error(RuntimeError)
        # Close the max_clients connections
        for i in 1..@options[:max_clients]
          redis[i].quit
        end
        # Now the new connection will be successful
        redis = Redis.new({:host => instance.ip, :port => @redis_port, :password => credentials["password"]})
        redis.info.should be
        redis.quit if redis
      rescue => e
        p e
      end
      @node.unprovision(credentials["name"])
    end

    it "should unprovision successfully when reach the maximum number of clients" do
      credentials = @node.provision(:free)
      instance = @node.get_instance(credentials["name"])
      redis = []
      # Create max_clients connections
      for i in 1..@options[:max_clients]
        redis[i] = Redis.new({:host => instance.ip, :port => @redis_port, :password => credentials["password"]})
        redis[i].info
      end
      @node.unprovision(credentials["name"]).should == true
    end
  end

  describe "Node.timeout" do
    it "should raise exception when redis client response time is too long" do
      credentials = @node.provision(:free)
      instance = @node.get_instance(credentials["name"])
      class Redis
        alias :old_info :info
        def info(cmd = nil)
          sleep 3
          old_info(cmd)
        end
      end
      expect {@node.get_info(instance.ip, @redis_port, credentials["password"])}.should raise_error(VCAP::Services::Redis::RedisError)
      class Redis
        alias :info :old_info
      end
      @node.get_info(instance.ip, @redis_port, credentials["password"]).should be
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
          instance = @node.get_instance(credentials["name"])
          redis_echo(instance.ip, @redis_port, credentials["password"]).should == true
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
