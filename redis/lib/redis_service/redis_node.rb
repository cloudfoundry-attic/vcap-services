# Copyright (c) 2009-2011 VMware, Inc.
require "erb"
require "fileutils"
require "logger"
require "pp"

require "uuidtools"
require "redis"
require "thread"

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
require "redis_service/util"

class VCAP::Services::Redis::Node

  include VCAP::Services::Redis::Common
  include VCAP::Services::Redis::Util
  include VCAP::Services::Redis

  class ProvisionedService
    include DataMapper::Resource
    property :name,       String,   :key => true
    property :port,       Integer,  :unique => true
    property :password,   String,   :required => true
    # property plan is deprecated. The instances in one node have same plan.
    property :plan,       Integer,  :required => true
    property :pid,        Integer
    property :memory,     Integer

    def listening?
      begin
        TCPSocket.open("localhost", port).close
        return true
      rescue => e
        return false
      end
    end

    def running?
      VCAP.process_running? pid
    end

    def kill(sig=:SIGTERM)
      @wait_thread = Process.detach(pid)
      Process.kill(sig, pid) if running?
    end

    def wait_killed(timeout=5, interval=0.2)
      begin
        Timeout::timeout(timeout) do
          @wait_thread.join if @wait_thread
          while running? do
            sleep interval
          end
        end
      rescue Timeout::Error
        return false
      end
      true
    end
  end

  def initialize(options)
    super(options)

    @base_dir = options[:base_dir]
    FileUtils.mkdir_p(@base_dir)
    @redis_server_path = options[:redis_server_path]
    @max_memory = options[:max_memory]
    @max_swap = options[:max_swap]
    @config_template = ERB.new(File.read(options[:config_template]))
    @free_ports = Set.new
    @free_ports_mutex = Mutex.new
    options[:port_range].each {|port| @free_ports << port}
    @local_db = options[:local_db]
    @disable_password = "disable-#{UUIDTools::UUID.random_create.to_s}"
    @redis_log_dir = options[:redis_log_dir]
    @config_command_name = @options[:command_rename_prefix] + "-config"
    @shutdown_command_name = @options[:command_rename_prefix] + "-shutdown"
    @save_command_name = @options[:command_rename_prefix] + "-save"
    @max_clients = @options[:max_clients] || 500
    # Timeout for redis client operations, node cannot be blocked on any redis instances.
    # Default value is 2 seconds.
    @redis_timeout = @options[:redis_timeout] || 2
    @redis_start_timeout = @options[:redis_start_timeout] || 3
  end

  def pre_send_announcement
    super
    start_db
    start_provisioned_instances
  end

  def shutdown
    super
    ProvisionedService.all.each do |instance|
      stop_redis_server(instance)
    end
    true
  end

  def announcement
    @capacity_lock.synchronize do
      { :available_capacity => @capacity,
        :capacity_unit => capacity_unit }
    end
  end

  def provision(plan, credentials = nil, db_file = nil)
    raise RedisError.new(RedisError::REDIS_INVALID_PLAN, plan) unless plan.to_s == @plan
    instance = ProvisionedService.new
    instance.plan = 1
    if credentials
      instance.name = credentials["name"]
      @free_ports_mutex.synchronize do
        if @free_ports.include?(credentials["port"])
          @free_ports.delete(credentials["port"])
          instance.port = credentials["port"]
        else
          port = @free_ports.first
          @free_ports.delete(port)
          instance.port = port
        end
      end
      instance.password = credentials["password"]
    else
      @free_ports_mutex.synchronize do
        port = @free_ports.first
        @free_ports.delete(port)
        instance.port = port
      end
      instance.name = UUIDTools::UUID.random_create.to_s
      instance.password = UUIDTools::UUID.random_create.to_s
    end

    begin
      instance.memory = memory_for_instance(instance)
    rescue => e
      raise e
    end
    begin
      instance.pid = start_instance(instance, db_file)
      save_instance(instance)
    rescue => e1
      begin
        cleanup_instance(instance)
      rescue => e2
        # Ignore the rollback exception
      end
      raise e1
    end

    # Sleep 1 second to wait for redis instance start
    sleep 1
    gen_credentials(instance)
  end

  def unprovision(instance_id, credentials_list = [])
    instance = get_instance(instance_id)
    cleanup_instance(instance)
    {}
  end

  def bind(instance_id, binding_options = :all, credentials = nil)
    # FIXME: Redis has no user level security, just return provisioned credentials.
    instance = nil
    if credentials
      instance = get_instance(credentials["name"])
    else
      instance = get_instance(instance_id)
    end
    gen_credentials(instance)
  end

  def unbind(credentials)
    # FIXME: Redis has no user level security, so has no operation for unbinding.
    {}
  end

  def restore(instance_id, backup_dir)
    instance = get_instance(instance_id)
    dump_file = File.join(backup_dir, "dump.rdb")
    if File.exists?(dump_file)
      if File.new(dump_file).size > 0
        stop_instance(instance) if instance.running?
        sleep 1
        instance.pid = start_instance(instance, dump_file)
        save_instance(instance)
      else
        Timeout::timeout(@redis_timeout) do
          redis = Redis.new({:port => instance.port, :password => instance.password})
          redis.flushall
        end
      end
    else
      raise RedisError.new(RedisError::REDIS_RESTORE_FILE_NOT_FOUND, dump_file)
    end
    {}
  end

  def disable_instance(service_credentials, binding_credentials_list = [])
    set_config(service_credentials["port"], service_credentials["password"], "requirepass", @disable_password)
    true
  rescue => e
    @logger.warn(e)
    nil
  end

  def enable_instance(service_credentials, binding_credentials_map = {})
    set_config(service_credentials["port"], @disable_password, "requirepass", service_credentials["password"])
    true
  rescue => e
    @logger.warn(e)
    nil
  end

  def update_instance(service_credentials, binding_credentials_map = {})
    instance = get_instance(service_credentials["name"])
    service_credentials = gen_credentials(instance)
    binding_credentials_map.each do |key, value|
      binding_credentials_map[key]["credentials"] = gen_credentials(instance)
    end
    [service_credentials, binding_credentials_map]
  rescue => e
    @logger.warn(e)
    nil
  end

  def dump_instance(service_credentials, binding_credentials_list = [], dump_dir)
    FileUtils.mkdir_p(dump_dir)
    instance = ProvisionedService.new
    instance.name = service_credentials["name"]
    instance.port = service_credentials["port"]
    instance.password = @disable_password
    dump_redis_data(instance, dump_dir)
  end

  def import_instance(service_credentials, binding_credentials_map={}, dump_dir, plan)
    db_file = File.join(dump_dir, "dump.rdb")
    provision(plan, service_credentials, db_file)
    true
  rescue => e
    @logger.warn(e)
    nil
  end

  def all_instances_list
    ProvisionedService.all.map{|ps| ps.name}
  end

  def varz_details
    varz = {}
    varz[:max_capacity] = @max_capacity
    varz[:available_capacity] = @capacity
    varz[:provisioned_instances] = []
    varz[:provisioned_instances_num] = 0
    ProvisionedService.all.each do |instance|
      varz[:provisioned_instances] << get_varz(instance)
      varz[:provisioned_instances_num] += 1
    end
    varz[:instances] = {}
    ProvisionedService.all.each do |instance|
      varz[:instances][instance.name.to_sym] = get_status(instance)
    end
    varz
  rescue => e
    @logger.warn("Error while getting varz details: #{e}")
    {}
  end

  def start_db
    DataMapper.setup(:default, @local_db)
    DataMapper::auto_upgrade!
  end

  def start_provisioned_instances
    @capacity_lock.synchronize do
      ProvisionedService.all.each do |instance|
        @free_ports_mutex.synchronize do
          @free_ports.delete(instance.port)
        end
        @capacity -= capacity_unit

        if instance.listening?
          @logger.warn("Service #{instance.name} already running on port #{instance.port}")
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
  end

  def save_instance(instance)
    raise RedisError.new(RedisError::REDIS_SAVE_INSTANCE_FAILED, instance.inspect) unless instance.save
    true
  end

  def destroy_instance(instance)
    raise RedisError.new(RedisError::REDIS_DESTORY_INSTANCE_FAILED, instance.inspect) unless instance.new? || instance.destroy
    true
  end

  def get_instance(name)
    instance = ProvisionedService.get(name)
    raise RedisError.new(RedisError::REDIS_FIND_INSTANCE_FAILED, name) if instance.nil?
    instance
  end

  def start_instance(instance, db_file = nil)
    @logger.debug("Starting: #{instance.inspect} on port #{instance.port}")

    pid = fork
    if pid
      @logger.debug("Service #{instance.name} started with pid #{pid}")
      # In parent, detch the child
      Process.detach(pid)
      # Wait enough time for the redis server starting
      @redis_start_timeout.times do
        sleep 1
        begin
          redis = Redis.new({:port => instance.port, :password => instance.password})
          redis.echo("")
          return pid
        rescue => e
          next
        ensure
          begin
            redis.quit if redis
          rescue => e
          end
        end
      end
      @logger.error("Timeout to start redis server for instance #{instance.name}")
      # Stop the instance if it is running
      instance.pid = pid
      stop_instance(instance) if instance.running?
      raise RedisError.new(RedisError::REDIS_START_INSTANCE_FAILED, instance.inspect)
    else
      $0 = "Starting Redis instance: #{instance.name}"
      close_fds

      memory = instance.memory
      port = instance.port
      password = instance.password
      dir = instance_dir(instance.name)
      data_dir = File.join(dir, "data")
      log_dir = instance_log_dir(instance.name)
      log_file = File.join(log_dir, "redis.log")
      swap_file = File.join(dir, "redis.swap")
      vm_max_memory = (memory * 0.7).round
      vm_pages = (@max_swap * 1024 * 1024 / 32).round # swap in bytes / size of page (32 bytes)
      config_command = @config_command_name
      shutdown_command = @shutdown_command_name
      save_command = @save_command_name
      maxclients = @max_clients

      config = @config_template.result(Kernel.binding)
      config_path = File.join(dir, "redis.conf")

      FileUtils.mkdir_p(dir)
      FileUtils.mkdir_p(data_dir)
      FileUtils.mkdir_p(log_dir)
      if db_file
        FileUtils.cp(db_file, data_dir)
      end
      FileUtils.rm_f(config_path)
      File.open(config_path, "w") {|f| f.write(config)}

      exec("#{@redis_server_path} #{config_path}")
    end
  rescue => e
    raise RedisError.new(RedisError::REDIS_START_INSTANCE_FAILED, instance.inspect)
  end

  def stop_instance(instance)
    stop_redis_server(instance)
    EM.defer do
      FileUtils.rm_rf(instance_dir(instance.name))
      FileUtils.rm_rf(instance_log_dir(instance.name))
    end
  end

  def cleanup_instance(instance)
    err_msg = []
    begin
      stop_instance(instance) if instance.running?
    rescue => e
      err_msg << e.message
    end
    @free_ports_mutex.synchronize do
      @free_ports.add(instance.port)
    end
    begin
      destroy_instance(instance)
    rescue => e
      err_msg << e.message
    end
    raise RedisError.new(RedisError::REDIS_CLEANUP_INSTANCE_FAILED, err_msg.inspect) if err_msg.size > 0
  end

  def memory_for_instance(instance)
    @max_memory
  end

  def get_varz(instance)
    info = get_info(instance.port, instance.password)
    varz = {}
    varz[:name] = instance.name
    varz[:port] = instance.port
    varz[:plan] = @plan
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

  def gen_credentials(instance)
    host = get_host
    credentials = {
      "hostname" => host,
      "host" => host,
      "port" => instance.port,
      "password" => instance.password,
      "name" => instance.name
    }
  end

  def get_status(instance)
    redis = nil
    Timeout::timeout(@redis_timeout) do
      redis = Redis.new({:port => instance.port, :password => instance.password})
      redis.echo("")
    end
    "ok"
  rescue => e
    "fail"
  ensure
    begin
      redis.quit if redis
    rescue => e
    end
  end

  def instance_dir(instance_id)
    File.join(@base_dir, instance_id)
  end

  def instance_log_dir(instance_id)
    File.join(@redis_log_dir, instance_id)
  end

end
