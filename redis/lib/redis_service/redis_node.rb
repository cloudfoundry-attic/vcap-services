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
        class ProvisionedService
        end
      end
    end
  end
end

class VCAP::Services::Redis::Node

  include VCAP::Services::Redis::Common
  include VCAP::Services::Redis::Util
  include VCAP::Services::Redis

  def initialize(options)
    super(options)
    @free_ports = Set.new
    @free_ports_lock = Mutex.new
    options[:port_range].each {|port| @free_ports << port}
    options[:max_clients] ||= 500
    options[:persistent] ||= false
    # Configuration used in warden
    options[:instance_base_dir] = "/store/instance"
    options[:instance_data_dir] = "/store/instance/data"
    options[:instance_log_dir] = "/store/log"
    options[:instance_migration_dir] = "/store/migration"
    @redis_port = options[:instance_port] = 25001
    @config_command_name = options[:config_command_name] = options[:command_rename_prefix] + "-config"
    @shutdown_command_name = options[:shutdown_command_name] = options[:command_rename_prefix] + "-shutdown"
    @save_command_name = options[:save_command_name] = options[:command_rename_prefix] + "-save"
    @disable_password = "disable-#{UUIDTools::UUID.random_create.to_s}"
    # Timeout for redis client operations, node cannot be blocked on any redis instances.
    # Default value is 2 seconds.
    @redis_timeout = @options[:redis_timeout] || 2
    ProvisionedService.init(options)
    @options = options
  end

  def pre_send_announcement
    @capacity_lock.synchronize do
      ProvisionedService.all.each do |instance|
        @capacity -= capacity_unit
        del_port(instance.port)

        if instance.ip == nil && instance.container == nil
          # This is an old redis instance, should migrate it to use warden
          @logger.info("Migrate an old redis instance #{instance.name}")
          data_source_file = File.join(instance.base_dir, "data", "dump.rdb")
          data_backup_file = "/tmp/#{instance.name}.dump.rdb"
          if File.exist?(data_source_file)
            FileUtils.cp(data_source_file, data_backup_file)
            instance = ProvisionedService.create(instance.port, instance.plan, instance.name, instance.password, data_backup_file)
            FileUtils.rm_f(data_backup_file)
          else
            instance = ProvisionedService.create(instance.port, instance.plan, instance.name, instance.password)
          end
        end

        if instance.running? then
          @logger.warn("Service #{instance.name} already listening on port #{instance.port}")
          next
        end

        unless instance.base_dir?
          @logger.warn("Service #{instance.name} in local DB, but not in file system")
          next
        end

        begin
          instance.run
          @logger.info("Successfully start provisioned instance #{instance.name}")
        rescue => e
          @logger.error("Error starting instance #{instance.name}: #{e}")
          instance.stop
        end
      end
    end
  end

  def shutdown
    super
    @logger.info("Shutting down instances..")
    ProvisionedService.all.each do |instance|
      @logger.debug("Try to terminate redis container: #{instance.name}")
      instance.stop
    end
  end

  def announcement
    @capacity_lock.synchronize do
      { :available_capacity => @capacity,
        :capacity_unit => capacity_unit }
    end
  end

  def provision(plan = nil, credentials = nil, db_file = nil)
    port = nil
    instance = nil
    if credentials
      port = new_port(credentials["port"])
      instance = ProvisionedService.create(port, plan, credentials["name"], credentials["password"], db_file)
    else
      port = new_port
      instance = ProvisionedService.create(port, plan, db_file)
    end
    instance.run
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
    instance = get_instance(service_credentials["name"])
    instance.password = @disable_password
    dump_redis_data(instance)
    FileUtils.cp(File.join(instance.data_dir, "dump.rdb"), dump_dir)
    true
  end

  def import_instance(service_credentials, binding_credentials_map={}, dump_dir, plan)
    db_file = File.join(dump_dir, "dump.rdb")
    provision(plan, service_credentials, db_file)
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

  def new_port(port=nil)
    @free_ports_lock.synchronize do
      raise "No ports available." if @free_ports.empty?
      if port.nil? || !@free_ports.include?(port)
        port = @free_ports.first
        @free_ports.delete(port)
      else
        @free_ports.delete(port)
      end
    end
    port
  end

  def free_port(port)
    @free_ports_lock.synchronize do
      raise "port #{port} already freed!" if @free_ports.include?(port)
      @free_ports.add(port)
    end
  end

  def del_port(port)
    @free_ports_lock.synchronize do
      @free_ports.delete(port)
    end
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

  property :name,       String,   :key => true
  property :port,       Integer,  :unique => true
  property :password,   String,   :required => true
  # property plan is deprecated. The instances in one node have same plan.
  property :plan,       Integer,  :required => true
  property :pid,        Integer
  property :memory,     Integer
  property :container,  String
  property :ip,         String

  private_class_method :new

  class << self

    include VCAP::Services::Redis

    def init(options)
      @@options = options
      FileUtils.mkdir_p(options[:base_dir])
      FileUtils.mkdir_p(options[:redis_log_dir])
      FileUtils.mkdir_p(options[:image_dir])
      @@warden_client = Warden::Client.new("/tmp/warden.sock")
      @@warden_client.connect
      @@warden_lock = Mutex.new
      # Some system commands like iptables will fail where there are multiple commands running in the same time
      # The default retry count is 5
      @@sysytem_command_retry_count = 5
      DataMapper.setup(:default, options[:local_db])
      DataMapper::auto_upgrade!
    end

    def create(port, plan=nil, name=nil, password=nil, db_file=nil)
      raise "Parameter missing" unless port
      # The instance could be an old instance without warden support
      instance = get(name) if name
      instance = new if instance == nil
      instance.port      = port
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
      swap_file = File.join(@@options[:instance_base_dir], "redis.swap")
      memory = instance.memory
      vm_max_memory = (instance.memory * 0.7).round
      vm_pages = (@@options[:max_swap] * 1024 * 1024 / 32).round # swap in bytes / size of page (32 bytes)
      config_command = @@options[:config_command_name]
      shutdown_command = @@options[:shutdown_command_name]
      save_command = @@options[:save_command_name]
      maxclients = @@options[:max_clients]
      config_template = ERB.new(File.read(@@options[:config_template]))
      config = config_template.result(Kernel.binding)
      config_path = File.join(instance.base_dir, "redis.conf")
      begin
        Open3.capture3("sudo umount #{instance.base_dir}") if File.exist?(instance.base_dir)
      rescue => e
      end
      FileUtils.rm_rf(instance.base_dir)
      FileUtils.rm_rf(instance.log_dir)
      FileUtils.rm_rf(instance.image_file)
      FileUtils.mkdir_p(instance.base_dir)
      # Mount base directory to loop device for disk size limitation
      cmd = "dd if=/dev/null of=#{instance.image_file} bs=1M seek=#{@@options[:max_db_size]}"
      o, e, s = Open3.capture3(cmd)
      raise RedisError.new(RedisError::REDIS_RUN_SYSTEM_COMMAND_FAILED, cmd, o, e) if s.exitstatus != 0
      cmd = "mkfs.ext4 -q -F -O \"^has_journal,uninit_bg\" #{instance.image_file}"
      o, e, s = Open3.capture3(cmd)
      raise RedisError.new(RedisError::REDIS_RUN_SYSTEM_COMMAND_FAILED, cmd, o, e) if s.exitstatus != 0
      cmd = "sudo mount -n -o loop #{instance.image_file} #{instance.base_dir}"
      o, e, s = Open3.capture3(cmd)
      raise RedisError.new(RedisError::REDIS_RUN_SYSTEM_COMMAND_FAILED, cmd, o, e) if s.exitstatus != 0
      FileUtils.mkdir_p(instance.data_dir)
      FileUtils.mkdir_p(instance.log_dir)
      if db_file
        FileUtils.cp(db_file, File.join(instance.data_dir, "dump.rdb"))
      end
      File.open(config_path, "w") {|f| f.write(config)}
      instance
    end
  end

  def delete
    # stop container
    stop if running?
    # delete log and service directory
    cmd = "sudo umount #{base_dir}"
    o, e, s = Open3.capture3(cmd)
    raise RedisError.new(RedisError::REDIS_RUN_SYSTEM_COMMAND_FAILED, cmd, o, e) if s.exitstatus != 0
    FileUtils.rm_rf(base_dir)
    FileUtils.rm_rf(log_dir)
    FileUtils.rm_rf(image_file)
    # delete recorder
    destroy!
  end

  def running?
    if (self[:container] == "")
      return false
    else
      @@warden_lock.synchronize do
        @@warden_client.call(["info", self[:container]])
      end
      return true
    end
  rescue => e
    return false
  end

  def stop
    unmapping_port(self[:ip], self[:port])
    @@warden_lock.synchronize do
      @@warden_client.call(["stop", self[:container]])
      @@warden_client.call(["destroy", self[:container]])
    end
    self[:container] = ""
    save
    true
  end

  def run
    @@warden_lock.synchronize do
      req = ["create", {"bind_mounts" => [[base_dir, @@options[:instance_base_dir], {"mode" => "rw"}], [log_dir, @@options[:instance_log_dir], {"mode" => "rw"}], [@@options[:migration_nfs], @@options[:instance_migration_dir], {"mode" => "rw"}]]}]
      self[:container] = @@warden_client.call(req)
      req = ["info", self[:container]]
      info = @@warden_client.call(req)
      self[:ip] = info["container_ip"]
    end
    save!
    mapping_port(self[:ip], self[:port])
    true
  end

  def mapping_port(ip, port)
    rule = [ "--protocol tcp",
             "--dport #{port}",
             "--jump DNAT",
             "--to-destination #{ip}:#{@@options[:instance_port]}" ]
    cmd = "sudo iptables -t nat -A PREROUTING #{rule.join(" ")}"
    retry_system_command(cmd)
  end

  def unmapping_port(ip, port)
    rule = [ "--protocol tcp",
             "--dport #{port}",
             "--jump DNAT",
             "--to-destination #{ip}:#{@@options[:instance_port]}" ]
    cmd = "sudo iptables -t nat -D PREROUTING #{rule.join(" ")}"
    retry_system_command(cmd)
  end

  def retry_system_command(cmd)
    for i in 1..@@sysytem_command_retry_count do
      o, e, s = Open3.capture3(cmd)
      return if s.exitstatus == 0
      raise RedisError.new(RedisError::REDIS_RUN_SYSTEM_COMMAND_FAILED, cmd, o, e) if i == @@sysytem_command_retry_count
      sleep 0.1
    end
  end

  # diretory helper
  def base_dir
    File.join(@@options[:base_dir], self[:name])
  end

  def base_dir?
    Dir.exists?(base_dir)
  end

  def data_dir
    File.join(base_dir, "data")
  end

  def log_dir
    File.join(@@options[:redis_log_dir], self[:name])
  end

  def image_file
     File.join(@@options[:image_dir], "#{self[:name]}.img")
  end
end
