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
      @node.start_db.should be_nil
      @node.logger.level = Logger::DEBUG
      @node.local_db = @options[:local_db]
    end

    it "should setup local db with right arguments" do
      @node.start_db.should be
    end
  end

#  describe 'Node.start_services' do
#    it "should check whether provisioned service is running or not" do
#      EM.run do
#        @service.pid = @node.start_instance(@service)
#        EM.add_timer(1) {
#          @service.running?.should == true
#          @node.stop_instance(@service)
#        }
#        EM.add_timer(2) {
#          @service.running?.should == false
#          EM.stop
#        }
#      end
#    end
#
#    it "should not start a new instance if the service is already started when start all provisioned services" do
#      EM.run do
#        @service.pid = @node.start_instance(@service)
#        @service.save
#        EM.add_timer(1) {
#          @node.start_services
#          service = VCAP::Services::Redis::Node::ProvisionedService.get(@service.name)
#          service.pid.should == @service.pid
#          @node.stop_instance(@service)
#          @service.destroy
#        }
#        EM.add_timer(2) {EM.stop}
#      end
#    end
#
#    it "should start a new instance if the service is not started when start all provisioned services" do
#      EM.run do
#        @service.pid = @node.start_instance(@service)
#        @service.save
#        @node.stop_instance(@service)
#        EM.add_timer(1) {
#          @node.start_services
#          service = VCAP::Services::Redis::Node::ProvisionedService.get(@service.name)
#          service.pid.should_not == @service.pid
#          @node.stop_instance(@service)
#          @service.destroy
#        }
#        EM.add_timer(2) {EM.stop}
#      end
#    end
#  end
#
#  describe 'Node.announcement' do
#    it "should send node announcement" do
#      @node.announcement.should be
#    end
#
#    it "should send available_memory in announce message" do
#      @node.announcement[:available_memory].should == @node.available_memory
#    end
#  end
#
#  describe "Node.provision" do
#    it "should access the service instance using the credentials returned by sucessful provision" do
#      @service.pid = @node.start_instance(@service)
#      EM.run do
#        EM.add_timer(1) {
#          %x[#{@options[:redis_client_path]} -p #{@service.port} -a #{@service.password} get test].should == "\n"
#          @node.stop_instance(@service)
#        }
#        EM.add_timer(2) {EM.stop}
#      end
#    end
#
#    it "should delete the provisioned service port in free port list when finish a provision" do
#      response = @node.provision(:free)
#      @node.free_ports.include?(response["port"]).should == false
#      @node.unprovision(response["name"])
#    end
#
#    it "should decrease available memory when finish a provision" do
#      old_memory = @node.available_memory
#      response = @node.provision(:free)
#      (old_memory - @node.available_memory).should == @node.max_memory
#      @node.unprovision(response["name"])
#    end
#
#    it "should send provision messsage when finish a provision" do
#      response = @node.provision(:free)
#      response["hostname"].should be
#      response["port"].should be
#      response["password"].should be
#      response["name"].should be
#      @node.unprovision(response["name"])
#    end
#  end
#
#  describe "Node.on_unprovision" do
#    it "should stop the redis server instance when doing unprovision" do
#      @service.pid      = @node.start_instance(@service)
#      EM.run do
#        EM.add_timer(1) {
#          @node.stop_instance(@service)
#          %x[#{@options[:redis_client_path]} -p #{@service.port} -a #{@service.password} get test].should_not == "\n"
#        }
#        EM.add_timer(2) { EM.stop }
#      end
#    end
#
#    it "should add the provisioned service port in free port list when finish an unprovision" do
#      response = @node.provision(:free)
#      @node.unprovision(response["name"])
#      @node.free_ports.include?(response["port"]).should == true
#    end
#
#    it "should increase available memory when finish an unprovision" do
#      response = @node.provision(:free)
#      old_memory = @node.available_memory
#      @node.unprovision(response["name"])
#      (@node.available_memory - old_memory).should == @node.max_memory
#    end
#
#    it "should raise error when unprovision an non-existed name" do
#      @node.logger.level = Logger::ERROR
#      @node.unprovision("non-existed")
#      @node.logger.level = Logger::DEBUG
#    end
#  end
#
#  describe "Node.save_service" do
#    it "shuold raise error when save failed" do
#      @service.pid = 100
#      @service.persisted_state=DataMapper::Resource::State::Immutable
#      begin
#        @node.save_service(@service)
#      rescue => e
#        e.should be
#      end
#    end
#  end
#
#  describe "Node.destory_service" do
#    it "shuold raise error when destroy failed" do
#      begin
#        @node.destroy_service(@service)
#      rescue => e
#        e.should be
#        @node.destroy_service(@service)
#      end
#    end
#  end
#
#  describe "Node.bind" do
#    before :all do
#      EM.run do
#        @response = @node.provision(:free)
#        EM.add_timer(1) {
#          EM.stop
#        }
#      end
#    end
#
#    after :all do
#      @node.unprovision(@response["name"])
#    end
#
#    it "should access redis server use the returned credential" do
#      handle = @node.bind(@response["name"])
#      %x[#{@options[:redis_client_path]} -p #{handle["port"]} -a #{handle["password"]} get test].should == "\n"
#      @node.unbind(handle)
#    end
#
#    it "should send binding messsage when finish a binding" do
#      handle = @node.bind(@response["name"])
#      handle["hostname"].should be
#      handle["port"].should be
#      handle["password"].should be
#      @node.unbind(handle)
#    end
#  end
#
#  describe "Node.unbind" do
#    before :all do
#      EM.run do
#        @response = @node.provision(:free)
#        EM.add_timer(1) {
#          EM.stop
#        }
#      end
#    end
#
#    after :all do
#      @node.unprovision(@response["name"])
#    end
#
#    it "should return true when finish an unbinding" do
#      handle = @node.bind(@response["name"])
#      @node.unbind(handle).should == true
#    end
#  end

  describe "Node.memory_for_service" do
    it "should return memory size by the plan" do
      service = VCAP::Services::Redis::Node::ProvisionedService.new
      service.plan = :free
      @node.memory_for_service(service).should == 16
    end
  end

  describe "Node.memory_for_service" do
    it "should raise ArgumentError when give wrong plan name" do
      service = VCAP::Services::Redis::Node::ProvisionedService.new
      service.plan = :non_existed_plan
      begin
        @node.memory_for_service(service)
      rescue => e
        e.class.should == ArgumentError
      end
    end
  end
end
