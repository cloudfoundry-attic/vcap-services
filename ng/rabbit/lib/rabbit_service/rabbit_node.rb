# Copyright (c) 2009-2011 VMware, Inc.
require "set"
require "open3"
require "uuidtools"
require "vcap/common"
require "vcap/component"
require "warden/client"
require "posix/spawn"
require "rabbit_service/common"
require "rabbit_service/rabbit_error"
require "rabbit_service/util"

module VCAP
  module Services
    module Rabbit
      class Node < VCAP::Services::Base::Node
        class ProvisionedService < VCAP::Services::Base::Warden::Service
        end
      end
    end
  end
end

VALID_CREDENTIAL_CHARACTERS = ("A".."Z").to_a + ("a".."z").to_a + ("0".."9").to_a
def generate_credential(length = 12)
  Array.new(length) {VALID_CREDENTIAL_CHARACTERS[rand(VALID_CREDENTIAL_CHARACTERS.length)]}.join
end

class VCAP::Services::Rabbit::Node

  include VCAP::Services::Rabbit::Common
  include VCAP::Services::Rabbit::Util
  include VCAP::Services::Rabbit
  include VCAP::Services::Base::Utils
  include VCAP::Services::Base::Warden::NodeUtils

  def initialize(options)
    super(options)
    init_ports(options[:port_range])
    options[:max_clients] ||= 500
    options[:vm_memory_high_watermark] ||= 0.0045
    options[:max_capacity] = @max_capacity
    # Default bin path for bandwidth proxy
    options[:proxy_bin] ||= "/var/vcap/packages/bandwidth_proxy/bin/bandwidth_proxy"
    # Default throughput limit is 1MB/day
    # Default limit window is 1 day
    options[:proxy_window] ||= 86400
    # Default limit size is 1 MB
    options[:proxy_limit] ||= 1
    # Configuration used in warden
    @rabbitmq_port = options[:service_port] = 10001
    @rabbitmq_admin_port = options[:service_admin_port] = 20001
    # Timeout for redis client operations, node cannot be blocked on any redis instances.
    # Default value is 2 seconds.
    @rabbitmq_timeout = @options[:rabbitmq_timeout] || 2
    @service_start_timeout = @options[:service_start_timeout] || 5
    @instance_parallel_start_count = 3
    @default_permissions = '{"configure":".*","write":".*","read":".*"}'
    options[:initial_username] = @initial_username = "guest"
    options[:initial_password] = @initial_password = "guest"
    @hostname = get_host
    ProvisionedService.init(options)
  end

  def pre_send_announcement
    start_all_instances
    @capacity_lock.synchronize{ @capacity -= ProvisionedService.all.size }
    warden_node_init(@options)
  end

  def service_instances
    ProvisionedService.all
  end

  def shutdown
    super
    @logger.info("Shutting down instances..")
    stop_all_instances
    true
  end

  def announcement
    @capacity_lock.synchronize do
      { :available_capacity => @capacity,
        :capacity_unit => capacity_unit }
    end
  end

  def provision(plan = nil, credentials = nil, version = nil)
    version ||= @options[:default_version]
    raise RabbitmqError.new(RabbitmqError::RABBITMQ_INVALID_PLAN, plan) unless plan.to_s == @plan
    raise ServiceError.new(ServiceError::UNSUPPORTED_VERSION, version) unless @supported_versions.include?(version)
    port = nil
    instance = nil

    if credentials
      port = new_port(credentials["port"])
      instance = ProvisionedService.create(port, get_admin_port(port), plan, credentials, version)
    else
      port = new_port
      instance = ProvisionedService.create(port, get_admin_port(port), plan, nil, version)
    end
    instance.run do
      # Use initial credentials to create provision user
      credentials = {"username" => @initial_username, "password" => @initial_password, "hostname" => instance.ip}
      add_vhost(credentials, instance.vhost)
      add_user(credentials, instance.admin_username, instance.admin_password)
      set_permissions(credentials, instance.vhost, instance.admin_username, @default_permissions)
      # Use provision user credentials to delete initial user for security
      credentials["username"] = instance.admin_username
      credentials["password"] = instance.admin_password
      delete_user(credentials, @initial_username)
    end
    @logger.info("Successfully fulfilled provision request: #{instance.name}")
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
    @logger.info("Successfully fulfilled unprovision request: #{name}")
    true
  end

  def bind(instance_id, binding_options = :all, binding_credentials = nil)
    instance = ProvisionedService.get(instance_id)
    user = nil
    pass = nil
    if binding_credentials
      user = binding_credentials["user"]
      pass = binding_credentials["pass"]
    else
      user = "u" + generate_credential
      pass = "p" + generate_credential
    end
    credentials = gen_admin_credentials(instance)
    add_user(credentials, user, pass)
    set_permissions(credentials, instance.vhost, user, get_permissions_by_options(binding_options))

    binding_credentials = gen_credentials(instance, user, pass)
    @logger.info("Successfully fulfilled bind request: #{binding_credentials}")
    binding_credentials
  rescue => e
    # Rollback
    begin
      delete_user(user)
    rescue => e1
      # Ignore the exception here
    end
    raise e
  end

  def unbind(credentials)
    instance = ProvisionedService.get(credentials["name"])
    delete_user(gen_admin_credentials(instance), credentials["user"])
    @logger.info("Successfully fulfilled unbind request: #{credentials}")
    {}
  end

  # Rabbitmq has no data to restore
  def restore(instance_id, backup_dir)
    true
  end

  def varz_details
    varz = super
    varz[:provisioned_instances] = []
    varz[:provisioned_instances_num] = 0
    varz[:max_capacity] = @max_capacity
    varz[:available_capacity] = @capacity
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
    @logger.warn(e)
    {}
  end

  def disable_instance(service_credentials, binding_credentials_list = [])
    @logger.info("disable_instance request: service_credentials=#{service_credentials}, binding_credentials=#{binding_credentials_list}")
    instance = ProvisionedService.get(service_credentials["name"])
    # Delete all binding users
    binding_credentials_list.each do |credentials|
      delete_user(gen_admin_credentials(instance), credentials["user"])
    end
    true
  rescue => e
    @logger.warn(e)
    nil
  end

  # Rabbitmq has no data to dump for migration
  def dump_instance(service_credentials, binding_credentials_list, dump_dir)
    true
  end

  def enable_instance(service_credentials, binding_credentials_map = {})
    @logger.info("enable_instance request: service_credentials=#{service_credentials}, binding_credentials=#{binding_credentials_map}")
    instance = ProvisionedService.get(service_credentials["name"])
    # Add all binding users
    binding_credentials_map.each do |_, value|
      bind(service_credentials["name"], value["binding_options"], value["credentials"])
    end
    true
  rescue => e
    @logger.warn(e)
    nil
  end

  def import_instance(service_credentials, binding_credentials_map, dump_dir, plan)
    provision(plan, service_credentials)
    true
  end

  def update_instance(service_credentials, binding_credentials_map={})
    instance = ProvisionedService.get(service_credentials["name"])
    service_credentials["hostname"] = @hostname
    service_credentials["host"] = @hostname
    binding_credentials_map.each do |key, value|
      bind(service_credentials["name"], value["binding_options"], value["credentials"])
      binding_credentials_map[key]["credentials"]["hostname"] = @hostname
      binding_credentials_map[key]["credentials"]["host"] = @hostname
    end
    [service_credentials, binding_credentials_map]
  rescue => e
    @logger.warn(e)
    nil
  end

  def all_instances_list
    ProvisionedService.all.map{|s| s.name}
  end

  def all_bindings_list
    res = []
    ProvisionedService.all.each do |instance|
      get_vhost_permissions(gen_admin_credentials(instance), instance.vhost).each do |entry|
        credentials = {
          "name" => instance.name,
          "hostname" => @hostname,
          "host" => @hostname,
          "port" => instance.port,
          "vhost" => instance.vhost,
          "username" => entry["user"],
          "user" => entry["user"],
        }
        res << credentials if credentials["username"] != instance.admin_username
      end
    end
    res
  end

  def get_varz(instance)
    varz = {}
    varz[:name] = instance.name
    varz[:plan] = @plan
    varz[:vhost] = instance.vhost
    varz[:admin_username] = instance.admin_username
    varz[:usage] = {}
    credentials = gen_admin_credentials(instance)
    varz[:usage][:queues_num] = list_queues(credentials, instance.vhost).size
    varz[:usage][:exchanges_num] = list_exchanges(credentials, instance.vhost).size
    varz[:usage][:bindings_num] = list_bindings(credentials, instance.vhost).size
    varz
  end

  def get_status(instance)
    get_permissions(gen_admin_credentials(instance), instance.vhost, instance.admin_username) ? "ok" : "fail"
  rescue => e
    "fail"
  end

  def gen_credentials(instance, user = nil, pass = nil)
    credentials = {
      "name" => instance.name,
      "hostname" => @hostname,
      "host" => @hostname,
      "port"  => instance.port,
      "vhost" => instance.vhost,
    }
    if user && pass # Binding request
      credentials["username"] = user
      credentials["user"] = user
      credentials["password"] = pass
      credentials["pass"] = pass
    else # Provision request
      credentials["username"] = instance.admin_username
      credentials["user"] = instance.admin_username
      credentials["password"] = instance.admin_password
      credentials["pass"] = instance.admin_password
    end
    credentials["url"] = "amqp://#{credentials["user"]}:#{credentials["pass"]}@#{credentials["host"]}:#{credentials["port"]}/#{credentials["vhost"]}"
    credentials
  end

  def gen_admin_credentials(instance)
    credentials = {
      "hostname"  => instance.ip,
      "username" => instance.admin_username,
      "password" => instance.admin_password,
    }
  end

  def get_admin_port(port)
    port + 10000
  end

  def get_instance(name)
    instance = ProvisionedService.get(name)
    raise RabbitmqError.new(RabbitmqError::RABBITMQ_FIND_INSTANCE_FAILED, name) if instance.nil?
    instance
  end

end

class VCAP::Services::Rabbit::Node::ProvisionedService

  include DataMapper::Resource
  include VCAP::Services::Rabbit
  include VCAP::Services::Rabbit::Util

  property :name,            String,      :key => true
  property :vhost,           String,      :required => true
  property :port,            Integer,     :unique => true
  property :admin_port,      Integer,     :unique => true
  property :admin_username,  String,      :required => true
  property :admin_password,  String,      :required => true
  # property plan is deprecated. The instances in one node have same plan.
  property :plan,            Integer,     :required => true
  property :plan_option,     String,      :required => false
  property :pid,             Integer
  property :proxy_pid,       Integer,     :required => true
  property :memory,          Integer,     :required => true
  property :status,          Integer,     :default => 0
  property :container,       String
  property :ip,              String
  property :version,         String,      :required => true

  private_class_method :new

  class << self

    def init(options)
      super
      @service_admin_port = @@options[:service_admin_port]
    end

    attr_reader :service_admin_port

    def create(port, admin_port, plan=nil, credentials=nil, version=nil)
      raise "Parameter missing" unless port && admin_port
      # The instance could be an old instance without warden support
      instance = get(credentials["name"]) if credentials
      instance = new if instance == nil
      instance.port = port
      instance.admin_port = port
      instance.version = (version || options[:default_version]).to_s
      instance.proxy_pid = 0
      if credentials
        instance.name = credentials["name"]
        instance.vhost = credentials["vhost"]
        instance.admin_username = credentials["username"]
        instance.admin_password = credentials["password"]
      else
        instance.name = UUIDTools::UUID.random_create.to_s
        instance.vhost = "v" + UUIDTools::UUID.random_create.to_s.gsub(/-/, "")
        instance.admin_username = "au" + generate_credential
        instance.admin_password = "ap" + generate_credential
      end
      # These properties are deprecated
      instance.memory = 1
      instance.plan = 1
      instance.plan_option = "rw"
      instance.pid = 0

      # Generate configuration
      port = @@options[:service_port]
      admin_port = @@options[:service_admin_port]
      vm_memory_high_watermark = @@options[:vm_memory_high_watermark]
      # In RabbitMQ, If the file_handles_high_watermark is x, then the socket limitation is trunc(x * 0.9) - 2,
      # to let the @max_clients be a more accurate limitation,
      # the file_handles_high_watermark will be set to ceil((@max_clients + 2) / 0.9)
      file_handles_high_watermark = ((@@options[:max_clients] + 2) / 0.9).ceil
      version_config = @@options[:rabbit][version.to_s]
      # Writes the RabbitMQ server erlang configuration file
      config_template = ERB.new(File.read(File.expand_path(version_config["config_template"], __FILE__)))
      config = config_template.result(Kernel.binding)
      config_path = File.join(instance.config_dir, "rabbitmq.config")
      begin
        Open3.capture3("umount #{instance.base_dir}") if File.exist?(instance.base_dir)
      rescue => e
      end
      FileUtils.rm_rf(instance.base_dir)
      FileUtils.rm_rf(instance.log_dir)
      FileUtils.rm_rf(instance.image_file)
      instance.prepare_filesystem(max_disk)
      FileUtils.mkdir_p(instance.config_dir)
      FileUtils.mkdir_p(instance.log_dir)
      # Writes the RabbitMQ server erlang configuration file
      File.open(config_path, "w") {|f| f.write(config)}
      # Enable management plugin
      File.open(File.join(instance.config_dir, "enabled_plugins"), "w") do |f|
        f.write <<EOF
[rabbitmq_management].
EOF
      end
      instance
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

  def start_options
    options = super
    options[:start_script] = {:script => "#{service_script} start #{base_dir} #{log_dir} #{common_dir} #{bin_dir} #{erlang_dir} #{name}", :use_spawn => true}
    options[:bind_dirs] << {:src => erlang_dir}
    options[:need_map_port] = false
    options
  end

  def erlang_dir
    erlang_bin_dir = self.class.bin_dir["erlang"]
    File.symlink?(erlang_bin_dir) ? File.readlink(erlang_bin_dir) : erlang_bin_dir
  end

  def finish_start?
    credentials = {"username" => admin_username, "password" => admin_password, "hostname" => ip}
    begin
      # Try to call management API, if success, then return
      response = create_resource(credentials)["users"].get
      JSON.parse(response)
      return true
    rescue => e
      return false
    end
  end

  def finish_first_start?
    credentials = {"username" => @@options[:initial_username], "password" => @@options[:initial_password], "hostname" => ip}
    begin
      # Try to call management API, if success, then return
      response = create_resource(credentials)["users"].get
      JSON.parse(response)
      return true
    rescue => e
      return false
    end
  end

  def run(options=nil, &post_start_block)
    super
    start_proxy
    save!
    true
  end

  def stop(container_name=nil)
    stop_proxy
    super(container_name)
  end

  def start_proxy
    self[:proxy_pid] = Process.fork do
      close_fds
      STDOUT.reopen(File.open("#{log_dir}/bandwidth_proxy_stdout.log", "a"))
      STDERR.reopen(File.open("#{log_dir}/bandwidth_proxy_stderr.log", "a"))
      exec(@@options[:proxy_bin],
           "-eport",  port.to_s,                         # External port proxy listen to
           "-iport",  @@options[:service_port].to_s,     # Internal port service listen to
           "-iip",    ip,                                # Internal ip service work on
           "-l",      "#{log_dir}/bandwidth_proxy.log",  # Log file
           "-window", @@options[:proxy_window].to_s,     # Time window to check for the transfer size(both in and out)
           "-limit",  (@@options[:proxy_limit] * 1024 * 1024).to_s)      # Limit size allowed every time window
    end
    Process.detach(self[:proxy_pid])
  end

  def stop_proxy
    Process.kill(:SIGTERM, self[:proxy_pid]) unless self[:proxy_pid] == 0
    # FIXME: should set proxy_pid to 0 in local db, but in unprovision we delete local db first,
    # and we don't know the operation is restart or unprovision here, so need consider a grace way to do it
  end

  def migration_check
    super
    if container == nil
      # Regenerate the configuration, need change the port to service_admin_port
      config_file = File.join(config_dir, "rabbitmq.config")
      content = File.read(config_file)
      content = content.gsub(/port, \d{5}/, "port, #{@@options[:service_admin_port]}")
      File.open(config_file, "w") {|f| f.write(content)}
    end
  end

  # diretory helper
  def config_dir
    File.join(base_dir, "config")
  end

end
