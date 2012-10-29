# Copyright (c) 2009-2011 VMware, Inc.
require "erb"
require "fileutils"
require "logger"
require "pp"

require "uuidtools"
require "redis"
require "thread"
require "open3"
require "vcap/common"
require "vcap/component"
require "warden/client"
require "redis_service/common"
require "redis_service/redis_error"
require "redis_service/util"

module VCAP
  module Services
    module Redis
      class Node < VCAP::Services::Base::Node
        class ProvisionedService < VCAP::Services::Base::WardenService
        end
      end
    end
  end
end

class VCAP::Services::Redis::Node

  include VCAP::Services::Redis::Common
  include VCAP::Services::Redis::Util
  include VCAP::Services::Redis
  include VCAP::Services::Base::Utils

  def initialize(options)
    super(options)
    init_ports(options[:port_range])
    options[:max_clients] ||= 500
    options[:persistent] ||= false
    # Configuration used in warden
    options[:instance_data_dir] = "/store/instance/data"
    options[:instance_log_dir] = "/store/log"
    @redis_port = options[:instance_port] = 25001
    @config_command_name = options[:config_command_name] = options[:command_rename_prefix] + "-config"
    @shutdown_command_name = options[:shutdown_command_name] = options[:command_rename_prefix] + "-shutdown"
    @save_command_name = options[:save_command_name] = options[:command_rename_prefix] + "-save"
    @disable_password = "disable-#{UUIDTools::UUID.random_create.to_s}"
    # Timeout for redis client operations, node cannot be blocked on any redis instances.
    # Default value is 2 seconds.
    @redis_timeout = @options[:redis_timeout] || 2
    @service_start_timeout = @options[:service_start_timeout] || 3
    ProvisionedService.init(options)
    @options = options
    @supported_versions = options[:supported_versions]
    @default_version = options[:default_version]
  end

  def migrate_saved_instances_on_startup
    ProvisionedService.all.each do |p_service|
      if p_service.version.to_s.empty?
        p_service.version = @default_version
        @logger.warn("Unable to set version for: #{p_service.inspect}") unless p_service.save
      end
    end
  end

  def pre_send_announcement
    migrate_saved_instances_on_startup

    @capacity_lock.synchronize do
      start_instances(ProvisionedService.all)
    end
  end

  def shutdown
    super
    @logger.info("Shutting down instances..")
    stop_instances(ProvisionedService.all)
    true
  end

  def announcement
    @capacity_lock.synchronize do
      { :available_capacity => @capacity,
        :capacity_unit => capacity_unit }
    end
  end

  def provision(plan = nil, credentials = nil, version = nil)
    @logger.info("Provision request: plan=#{plan}, version=#{version}")
    raise RedisError.new(RedisError::REDIS_INVALID_PLAN, plan) unless plan.to_s == @plan
    raise ServiceError.new(ServiceError::UNSUPPORTED_VERSION, version) unless @supported_versions.include?(version)

    port = nil
    instance = nil
    if credentials
      port = new_port(credentials["port"])
      instance = ProvisionedService.create(port, version, plan, credentials["name"], credentials["password"])
    else
      port = new_port
      instance = ProvisionedService.create(port, version, plan, nil, nil)
    end
    instance.run
    raise RedisError.new(RedisError::REDIS_START_INSTANCE_TIMEOUT, instance.name) if wait_service_start(instance) == false
    @logger.info("Successfully fulfilled provision request: #{instance.name}.")
    gen_credentials(instance)
  rescue => e
    @logger.error("Error provision instance: #{e}")
    instance.delete if instance
    free_port(port) if port
    raise e
  end

  def unprovision(name, credentials_list = [])
    instance = ProvisionedService.get(name)
    raise ServiceError.new(ServiceError::NOT_FOUND, name) if instance.nil?
    port = instance.port
    raise "Could not cleanup instance #{name}" unless instance.delete
    free_port(port)
    @logger.info("Successfully fulfilled unprovision request: #{name}.")
    true
  end

  def bind(name, binding_options = :all, credentials = nil)
    # FIXME: Redis has no user level security, just return provisioned credentials.
    @logger.info("Bind request: name=#{name}, binding_options=#{binding_options}")
    instance = ProvisionedService.get(name)
    raise "Could not find instance: #{name}" if instance.nil?
    gen_credentials(instance)
  end

  def unbind(credentials)
    # FIXME: Redis has no user level security, so has no operation for unbinding.
    {}
  end

  def varz_details
    varz = {}
    varz[:max_capacity] = @max_capacity
    varz[:available_capacity] = @capacity
    varz[:provisioned_instances] = []
    varz[:provisioned_instances_num] = 0
    varz[:instances] = {}
    ProvisionedService.all.each do |instance|
      varz[:instances][instance.name.to_sym] = get_status(instance)
      varz[:provisioned_instances_num] += 1
      begin
        varz[:provisioned_instances] << get_varz(instance)
      rescue => e
        @logger.warn("Failed to get instance #{instance.name} varz details: #{e}")
      end
    end
    varz
  rescue => e
    @logger.warn("Error while getting varz details: #{e}")
    {}
  end

  def all_instances_list
    ProvisionedService.all.map{|ps| ps.name}
  end

  def restore(instance_id, backup_dir)
    instance = get_instance(instance_id)
    dump_file = File.join(backup_dir, "dump.rdb")
    if File.exists?(dump_file)
      if File.new(dump_file).size > 0
        instance.stop if instance.running?
        sleep 1
        FileUtils.cp(dump_file, File.join(instance.data_dir, "dump.rdb"))
        instance.run
      else
        # No restore data in the dump file, so flush all the data in the instance
        redis = nil
        begin
          Timeout::timeout(@redis_timeout) do
            redis = Redis.new({:host => instance.ip, :port => @redis_port, :password => instance.password})
            redis.flushall
          end
        ensure
          begin
            redis.quit if redis
          rescue => e
          end
        end
      end
    else
      raise RedisError.new(RedisError::REDIS_RESTORE_FILE_NOT_FOUND, dump_file)
    end
    {}
  end

  def disable_instance(service_credentials, binding_credentials_list = [])
    instance = get_instance(service_credentials["name"])
    set_config(instance.ip, @redis_port, instance.password, "requirepass", @disable_password)
    true
  rescue => e
    @logger.warn(e)
    nil
  end

  def enable_instance(service_credentials, binding_credentials_map = {})
    instance = get_instance(service_credentials["name"])
    set_config(instance.ip, @redis_port, @disable_password, "requirepass", instance.password)
    true
  rescue => e
    @logger.warn(e)
    nil
  end

  def dump_instance(service_credentials, binding_credentials_list = [], dump_dir)
    FileUtils.mkdir_p(dump_dir)
    instance = get_instance(service_credentials["name"])
    raise "Cannot find provisioned service instance: #{service_credentials['name']}" if instance.nil?

    dump_file_name = File.join(dump_dir, 'dump_file')
    File.open(dump_file_name, 'w') do |f|
      Marshal.dump(instance, f)
    end

    instance.password = @disable_password
    dump_redis_data(instance)
    FileUtils.cp(File.join(instance.data_dir, "dump.rdb"), dump_dir)
    true
  end

  def import_instance(service_credentials, binding_credentials_map={}, dump_dir, plan)
    db_file = File.join(dump_dir, "dump.rdb")

    # Load provisioned_service from dumped file
    stored_service = nil
    dump_file_name = File.join(dump_dir, 'dump_file')
    File.open(dump_file_name, 'r') do |f|
      stored_service = Marshal.load(f)
    end
    raise "Cannot parse dumpfile stored_service in #{dump_file_name}" if stored_service.nil?

    port = new_port(stored_service.port)
    instance = ProvisionedService.create(
      new_port(stored_service.port),
      stored_service.version,
      stored_service.plan,
      stored_service.name,
      stored_service.password,
      db_file)
    instance.run
    raise RedisError.new(RedisError::REDIS_START_INSTANCE_TIMEOUT, instance.name) if wait_service_start(instance) == false

    true
  rescue => e
    @logger.warn(e)
    nil
  end

  def update_instance(service_credentials, binding_credentials_map = {})
    instance = get_instance(service_credentials["name"])
    service_credentials = gen_credentials(instance)
    binding_credentials_map.each do |key, _|
      binding_credentials_map[key]["credentials"] = gen_credentials(instance)
    end
    [service_credentials, binding_credentials_map]
  rescue => e
    @logger.warn(e)
    nil
  end

  def get_varz(instance)
    info = get_info(instance.ip, @redis_port, instance.password)
    varz = {}
    varz[:name] = instance.name
    varz[:port] = instance.port
    varz[:plan] = instance.plan
    varz[:version] = instance.version
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

  def get_status(instance)
    redis = nil
    Timeout::timeout(@redis_timeout) do
      redis = Redis.new({:host => instance.ip, :port => @redis_port, :password => instance.password})
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

  def is_service_started(instance)
    get_status(instance) == "ok" ? true : false
  end

  def get_instance(name)
    instance = ProvisionedService.get(name)
    raise RedisError.new(RedisError::REDIS_FIND_INSTANCE_FAILED, name) if instance.nil?
    instance
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

end

class VCAP::Services::Redis::Node::ProvisionedService

  include DataMapper::Resource
  include VCAP::Services::Redis

  property :name,       String,   :key => true
  property :port,       Integer,  :unique => true
  property :password,   String,   :required => true
  # property plan is deprecated. The instances in one node have same plan.
  property :plan,       Integer,  :required => true
  property :pid,        Integer
  property :memory,     Integer
  property :container,  String
  property :ip,         String

  # Making this false for backwards compatibility
  # Also Refer: get_version
  property :version,    String,   :required => false

  private_class_method :new

  class << self

    include VCAP::Services::Redis

    def init(options)
      super(options)
      @memory_limit = (options[:max_memory] || 128) + (options[:memory_overhead] || 0)
    end

    def create(port, version, plan=nil, name=nil, password=nil, db_file=nil, is_upgraded=false)
      raise "Parameter missing" unless port
      # The instance could be an old instance without warden support
      instance = get(name) if name
      instance = new if instance == nil
      instance.port      = port
      instance.version   = version
      instance.name      = name || UUIDTools::UUID.random_create.to_s
      instance.password  = password || UUIDTools::UUID.random_create.to_s
      # These three properties are deprecated
      instance.memory    = @@options[:max_memory]
      instance.plan      = 1
      instance.pid       = 0

      raise "Cannot save provision service" unless instance.save!

      # Generate configuration
      port = @@options[:instance_port]
      password = instance.password
      persistent = @@options[:persistent]
      data_dir = @@options[:instance_data_dir]
      log_file = File.join(@@options[:instance_log_dir], "redis.log")
      memory = instance.memory
      config_command = @@options[:config_command_name]
      shutdown_command = @@options[:shutdown_command_name]
      save_command = @@options[:save_command_name]
      maxclients = @@options[:max_clients]
      config_template = ERB.new(File.read(@@options[:config_template]))
      version = instance.version
      config = config_template.result(Kernel.binding)
      config_path = File.join(instance.base_dir, "redis.conf")

      if is_upgraded == false
        # Mount base directory to loop device for disk size limitation
        db_size = db_size || max_disk
        instance.prepare_filesystem(db_size)
        FileUtils.mkdir_p(instance.data_dir)
        if db_file
          FileUtils.cp(db_file, File.join(instance.data_dir, "dump.rdb"))
        end
      end
      File.open(config_path, "w") {|f| f.write(config)}
      instance
    end
  end

  def service_port
    25001
  end

  def service_script
    "redis_startup.sh #{version}"
  end

  def migration_check
    super
    # Need regenerate configuration file for redis server
    if container == nil
      VCAP::Services::Redis::Node::ProvisionedService.create(port, version, plan, name, password, nil, true)
    end
  end

  # diretory helper
  def data_dir
    File.join(base_dir, "data")
  end
end
