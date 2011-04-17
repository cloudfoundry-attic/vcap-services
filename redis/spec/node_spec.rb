require File.dirname(__FILE__) + '/spec_helper'

require 'redis_service/redis_node'

module VCAP
  module Services
    module Redis
      class Node
         attr_reader :base_dir, :redis_server_path, :redis_client_path, :local_ip, :available_memory, :max_memory, :max_swap, :node_id, :config_template, :free_ports
         attr_accessor :logger, :local_db
      end
    end
  end
end

describe VCAP::Services::Redis::Node do

  before :all do
    @logger = Logger.new(STDOUT, "daily")
    @logger.level = Logger::DEBUG
    @options = {
      :logger => @logger,
      :base_dir => "/var/vcap/services/redis/instances",
      :redis_server_path => "redis-server",
      :redis_client_path => "redis-cli",
      :local_ip => "127.0.0.1",
      :available_memory => 4096,
      :max_memory => 16,
      :max_swap => 32,
      :node_id => "redis-node-1",
      :config_template => File.expand_path("../resources/redis.conf.erb", File.dirname(__FILE__)),
      :local_db => "sqlite3:/tmp/redis_node.db",
      :port_range => Range.new(5000, 25000),
      :mbus => "nats://localhost:4222",
      :nfs_dir => "/tmp"
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
    @service          = VCAP::Services::Redis::Node::ProvisionedService.new
    @service.name     = "redis-#{UUIDTools::UUID.random_create.to_s}"
    @service.port     = 11111
    @service.plan     = :free
    @service.password = UUIDTools::UUID.random_create.to_s
    @service.memory   = @options[:max_memory]
  end

  after :all do
    # Stop NATS server
    if File.exists?(@pid_file)
      pid = File.read(@pid_file).chomp.to_i
      %x[kill -9 #{pid}]
      %x[rm -f #{@pid_file}]
    end
  end

  describe 'Node.initialize' do
    it "should set up a base directory" do
      @node.base_dir.should be @options[:base_dir]
    end

    it "should set up a redis server path" do
      @node.redis_server_path.should be @options[:redis_server_path]
    end

    it "should set up a redis client path" do
      @node.redis_client_path.should be @options[:redis_client_path]
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

  describe 'Node.start_provisioned_services' do
    it "should check whether provisioned service is running or not" do
      EM.run do
        @service.pid = @node.start_instance(@service)
        EM.add_timer(1) {
          @service.running?.should == true
          @node.stop_instance(@service)
        }
        EM.add_timer(2) {
          @service.running?.should == false
          EM.stop
        }
      end
    end

    it "should not start a new instance if the service is already started when start all provisioned services" do
      EM.run do
        @service.pid = @node.start_instance(@service)
        @service.save
        EM.add_timer(1) {
          @node.start_provisioned_services
          service = VCAP::Services::Redis::Node::ProvisionedService.get(@service.name)
          service.pid.should == @service.pid
          @node.stop_instance(@service)
          @service.destroy
        }
        EM.add_timer(2) {EM.stop}
      end
    end

    it "should start a new instance if the service is not started when start all provisioned services" do
      EM.run do
        @service.pid = @node.start_instance(@service)
        @service.save
        @node.stop_instance(@service)
        EM.add_timer(1) {
          @node.start_provisioned_services
          service = VCAP::Services::Redis::Node::ProvisionedService.get(@service.name)
          service.pid.should_not == @service.pid
          @node.stop_instance(@service)
          @service.destroy
        }
        EM.add_timer(2) {EM.stop}
      end
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
      @credentials = @node.provision(:free)[1]
      sleep 1
    end

    after :all do
      @node.unprovision(@credentials["name"])
    end

    it "should access the service instance using the credentials returned by sucessful provision" do
      %x[#{@options[:redis_client_path]} -p #{@credentials["port"]} -a #{@credentials["password"]} get test].should == "\n"
    end

    it "should not allow null credentials to access the service instance" do
      %x[#{@options[:redis_client_path]} -p #{@credentials["port"]} get test].should == "ERR operation not permitted\n"
    end

    it "should not allow wrong credentials to access the service instance" do
      %x[#{@options[:redis_client_path]} -p #{@credentials["port"]} -a wrong_password get test].should == "ERR operation not permitted\n"
    end

    it "should delete the provisioned service port in free port list when finish a provision" do
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
  end

  describe "Node.unprovision" do
    before :all do
      @credentials = @node.provision(:free)[1]
      @old_memory = @node.available_memory
      sleep 1
      @node.unprovision(@credentials["name"])
    end

    it "should not access the service instance when doing unprovision" do
      %x[#{@options[:redis_client_path]} -p #{@credentials["port"]} -a #{@credentials["password"]} get test].should_not == "\n"
    end

    it "should add the provisioned service port in free port list when finish an unprovision" do
      @node.free_ports.include?(@credentials["port"]).should == true
    end

    it "should increase available memory when finish an unprovision" do
      (@node.available_memory - @old_memory).should == @node.max_memory
    end

    it "should raise error when unprovision an non-existed name" do
      @node.logger.level = Logger::ERROR
      @node.unprovision("non-existed")[0].should == false
      @node.logger.level = Logger::DEBUG
    end
  end

  describe "Node.save_service" do
    it "shuold raise error when save service instance failed" do
      @service.pid = 100
      @service.persisted_state=DataMapper::Resource::State::Immutable
      begin
        @node.save_service(@service)
      rescue => e
        e.class.should == VCAP::Services::Redis::Node::RedisError
      end
    end
  end

  describe "Node.destory_service" do
    it "shuold raise error when destroy service instance failed" do
      begin
        @node.destroy_service(@service)
      rescue => e
        e.class.should == VCAP::Services::Redis::Node::RedisError
        @node.destroy_service(@service)
      end
    end
  end

  describe "Node.get_service" do
    it "shuold raise error when get service instance failed" do
      begin
        @node.get_service("non-existed")
      rescue => e
        e.class.should == VCAP::Services::Redis::Node::RedisError
      end
    end
  end

  describe "Node.bind" do
    before :all do
      @service_credentials = @node.provision(:free)[1]
      sleep 1
      @binding_credentials = @node.bind(@service_credentials["name"])[1]
      sleep 1
    end

    after :all do
      @node.unbind(@binding_credentials)
      sleep 1
      @node.unprovision(@service_credentials["name"])
    end

    it "should access redis server use the returned credential" do
      %x[#{@options[:redis_client_path]} -p #{@binding_credentials["port"]} -a #{@binding_credentials["password"]} get test].should == "\n"
    end

    it "should not allow null credentials to access the service instance" do
      %x[#{@options[:redis_client_path]} -p #{@binding_credentials["port"]} get test].should == "ERR operation not permitted\n"
    end

    it "should not allow wrong credentials to access the service instance" do
      %x[#{@options[:redis_client_path]} -p #{@binding_credentials["port"]} -a wrong_password get test].should == "ERR operation not permitted\n"
    end

    it "should send binding messsage when finish a binding" do
      @binding_credentials["hostname"].should be
      @binding_credentials["port"].should be
      @binding_credentials["password"].should be
    end
  end

  describe "Node.unbind" do
    it "should return true when finish an unbinding" do
      @service_credentials = @node.provision(:free)[1]
      sleep 1
      @binding_credentials = @node.bind(@service_credentials["name"])[1]
      sleep 1
      @node.unbind(@binding_credentials)[0].should == true
      @node.unprovision(@service_credentials["name"])
    end
  end

  describe "Node.memory_for_service" do
    it "should return memory size by the plan" do
      service = VCAP::Services::Redis::Node::ProvisionedService.new
      service.plan = :free
      @node.memory_for_service(service).should == 16
    end

    it "should raise ArgumentError when give wrong plan name" do
      service = VCAP::Services::Redis::Node::ProvisionedService.new
      service.plan = :non_existed_plan
      begin
        @node.memory_for_service(service)
      rescue => e
        e.class.should == VCAP::Services::Redis::Node::RedisError
      end
    end
  end
end
