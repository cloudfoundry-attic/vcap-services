# Copyright (c) 2009-2011 VMware, Inc.
require "erb"
require "fileutils"
require "logger"
require "pp"

require "datamapper"
require "uuidtools"
require "redis"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..', 'base', 'lib')
require 'base/node'

module VCAP
  module Services
    module Redis
      class Node < VCAP::Services::Base::Node
      end
    end
  end
end

require "redis_service/common"
require "redis_service/redis_error"

class VCAP::Services::Redis::Node

  include VCAP::Services::Redis::Common
  include VCAP::Services::Redis

  class ProvisionedInstance
    include DataMapper::Resource
    property :name,       String,   :key => true
    property :port,       Integer,  :unique => true
    property :password,   String,   :required => true
    property :plan,       Enum[:free], :required => true
    property :pid,        Integer
    property :memory,     Integer

    def listening?
      begin
        TCPSocket.open('localhost', port).close
        return true
      rescue => e
        return false
      end
    end

    def running?
      VCAP.process_running? pid
    end
  end

  def initialize(options)
    super(options)

    @base_dir = options[:base_dir]
    FileUtils.mkdir_p(@base_dir)
    @redis_server_path = options[:redis_server_path]
    @available_memory = options[:available_memory]
    @max_memory = options[:max_memory]
    @max_swap = options[:max_swap]
    @config_template = ERB.new(File.read(options[:config_template]))
    @free_ports = Set.new
    options[:port_range].each {|port| @free_ports << port}
    @local_db = options[:local_db]
    @nfs_dir = options[:nfs_dir]
    @disable_password = "disable-#{UUIDTools::UUID.random_create.to_s}"
    @options = options
  end

  def start
    @logger.info("Starting redis node...")
    start_db
    start_provisioned_instances
    true
  end

  def shutdown
    super if defined?(super)
    ProvisionedInstance.all.each do |instance|
      stop_redis_server(instance)
    end
    true
  end

  def announcement
    a = {
      :available_memory => @available_memory
    }
  end

  def provision(plan)
    port = @free_ports.first
    @free_ports.delete(port)

    instance          = ProvisionedInstance.new
    instance.name     = "redis-#{UUIDTools::UUID.random_create.to_s}"
    instance.port     = port
    instance.plan     = plan
    instance.password = UUIDTools::UUID.random_create.to_s
    begin
      instance.memory = memory_for_instance(instance)
      @available_memory -= instance.memory
    rescue => e
      raise e
    end
    begin
      instance.pid = start_instance(instance)
      save_instance(instance)
    rescue => e1
      begin
        cleanup_instance(instance)
      rescue => e2
        # Ignore the rollback exception
      end
      raise e1
    end

    credentials = {
      "hostname" => @local_ip,
      "port" => instance.port,
      "password" => instance.password,
      "name" => instance.name
    }
  end

  def unprovision(instance_id, credentials_list = [])
    instance = get_instance(instance_id)
    cleanup_instance(instance)
    {}
  end

  def bind(instance_id, binding_options = :all)
    # FIXME: Redis has no user level security, just return provisioned credentials.
    instance = get_instance(instance_id)
    credentials = {
      "hostname" => @local_ip,
      "port" => instance.port,
      "password" => instance.password
    }
  end

  def unbind(credentials)
    # FIXME: Redis has no user level security, so has no operation for unbinding.
    {}
  end

  def varz_details
    varz = {}
    varz[:provisioned_instances] = []
    varz[:provisioned_instances_num] = 0
    varz[:max_instances_num] = @options[:available_memory] / @max_memory
    ProvisionedInstance.all.each do |instance|
      varz[:provisioned_instances] << get_varz(instance)
      varz[:provisioned_instances_num] += 1
    end
    varz
  rescue => e
    @logger.warn("Error get varz details: #{e}")
    {}
  end

  def start_db
    DataMapper.setup(:default, @local_db)
    DataMapper::auto_upgrade!
  end

  def start_provisioned_instances
    ProvisionedInstance.all.each do |instance|
      @free_ports.delete(instance.port)
      if instance.listening?
        @logger.info("Service #{instance.name} already running on port #{instance.port}")
        @available_memory -= (instance.memory || @max_memory)
        next
      end
      begin
        pid = start_instance(instance)
        instance.pid = pid
        save_instance(instance)
      rescue => e
        @logger.warn("Error starting instance #{instance.name}: #{e}")
        begin
          cleanup_instance(instance)
        rescue => e2
          # Ignore the rollback exception
        end
      end
    end
  end

  def save_instance(instance)
    raise RedisError.new(RedisError::REDIS_SAVE_INSTANCE_FAILED, instance.pretty_inspect) unless instance.save
  end

  def destroy_instance(instance)
    raise RedisError.new(RedisError::REDIS_DESTORY_INSTANCE_FAILED, instance.pretty_inspect) unless instance.destroy
  end

  def get_instance(name)
    instance = ProvisionedInstance.get(name)
    raise RedisError.new(RedisError::REDIS_FIND_INSTANCE_FAILED, name) if instance.nil?
    instance
  end

  def start_instance(instance, db_file = nil)
    @logger.debug("Starting: #{instance.pretty_inspect} on port #{instance.port}")

    pid = fork
    if pid
      @logger.debug("Service #{instance.name} started with pid #{pid}")
      # In parent, detch the child.
      Process.detach(pid)
      pid
    else
      $0 = "Starting Redis instance: #{instance.name}"
      close_fds

      memory = instance.memory
      port = instance.port
      password = instance.password
      dir = File.join(@base_dir, instance.name)
      data_dir = File.join(dir, "data")
      log_file = File.join(dir, "log")
      swap_file = File.join(dir, "redis.swap")
      vm_max_memory = (memory * 0.7).round
      vm_pages = (@max_swap * 1024 * 1024 / 32).round # swap in bytes / size of page (32 bytes)

      config = @config_template.result(Kernel.binding)
      config_path = File.join(dir, "redis.conf")

      FileUtils.mkdir_p(dir)
      FileUtils.mkdir_p(data_dir)
      if db_file
        FileUtils.cp(db_file, data_dir)
      end
      FileUtils.rm_f(log_file)
      FileUtils.rm_f(config_path)
      File.open(config_path, "w") {|f| f.write(config)}

      exec("#{@redis_server_path} #{config_path}")
    end
  rescue => e
    raise RedisError.new(RedisError::REDIS_START_INSTANCE_FAILED, instance.pretty_inspect)
  end

  def stop_instance(instance)
    stop_redis_server(instance)
    dir = File.join(@base_dir, instance.name)
    EM.defer {FileUtils.rm_rf(dir)}
  end

  def cleanup_instance(instance)
    err_msg = []
    begin
      stop_instance(instance) if instance.running?
    rescue => e
      err_msg << e.message
    end
    @available_memory += instance.memory
    @free_ports.add(instance.port)
    begin
      destroy_instance(instance)
    rescue => e
      err_msg << e.message
    end
    raise RedisError.new(RedisError::REDIS_CLEANUP_INSTANCE_FAILED, err_msg.inspect) if err_msg.size > 0
  end

  def memory_for_instance(instance)
    case instance.plan
      when :free then 16
      else
        raise RedisError.new(RedisError::REDIS_INVALID_PLAN, instance.plan)
    end
  end

  def stop_redis_server(instance)
    redis = Redis.new({:port => instance.port, :password => instance.password})
    begin
      redis.shutdown
    rescue => e
      # FIXME: it will raise exception even if shutdown successfully,
      # should be a redis ruby binding bug. Here just ignore it.
    end
  end

  def close_fds
    3.upto(get_max_open_fd) do |fd|
      begin
        IO.for_fd(fd, "r").close
      rescue
      end
    end
  end

  def get_max_open_fd
    max = 0

    dir = nil
    if File.directory?("/proc/self/fd/") # Linux
      dir = "/proc/self/fd/"
    elsif File.directory?("/dev/fd/") # Mac
      dir = "/dev/fd/"
    end

    if dir
      Dir.foreach(dir) do |entry|
        begin
          pid = Integer(entry)
          max = pid if pid > max
        rescue
        end
      end
    else
      max = 65535
    end

    max
  end

  def get_info(instance)
    redis = Redis.new({:port => instance.port, :password => instance.password})
    redis.info
  rescue => e
    raise RedisError.new(RedisError::REDIS_GET_INSTANCE_INFO_FAILED, instance.pretty_inspect)
  end

  def get_varz(instance)
    info = get_info(instance)
    varz = {}
    varz[:name] = instance.name
    varz[:port] = instance.port
    varz[:plan] = instance.plan
    varz[:usage] = {}
    varz[:usage][:max_memory] = instance.memory.to_f * 1024.0
    varz[:usage][:used_memory] = info["used_memory"].to_f / (1024.0 * 1024.0)
    varz[:usage][:max_virtual_memory] = info["vm_conf_max_memory"].to_f / 1024.0
    varz[:usage][:used_virtual_memory] = info["vm_stats_used_pages"].to_f * info["vm_conf_page_size"].to_f / (1024.0 * 1024.0)
    varz[:usage][:connected_clients_num] = info["connected_clients"].to_i
    varz[:usage][:last_save_time] = info["last_save_time"].to_i
    varz[:usage][:bgsave_in_progress] = (info["bgsave_in_progress"] == "0" ? false : true)
    varz
  end

end
