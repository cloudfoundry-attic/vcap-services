# Copyright (c) 2009-2011 VMware, Inc.
# This code is based on Redis as a Service.

require "erb"
require "fileutils"
require "logger"

require "uuidtools"
require 'dalli'
require "thread"

module VCAP
  module Services
    module Memcached
      class Node < VCAP::Services::Base::Node
      end
    end
  end
end

require "memcached_service/common"
require "memcached_service/memcached_error"

class VCAP::Services::Memcached::Node

  include VCAP::Services::Memcached::Common
  include VCAP::Services::Memcached

  class SASLAdmin
    class SASLOperationError < StandardError
      SASL_OPS_USER_ALREADY_EXISTS  = 'Failed to create user. Specified user has already exists.'
      SASL_OPS_ILLEGAL_INPUT        = 'Illegal input.'
      SASL_OPS_UNKNOWN_ERROR        = 'Failed to create user. Unknown error.'
    end

    def initialize(logger)
      @logger = logger
    end

    def user_list
      list_str = `sasldblistusers2`.split
      list_str.delete('userPassword')
      users = list_str.map do |e|
        separator = e.index('@') - 1
        e.slice(0..separator)
      end

      return users
    end

    def create_user(user, password)
      if user.nil? || user.empty?
        raise SASLOperationError::SASL_OPS_ILLEGAL_INPUT
      end

      users = user_list()

      if users.include?(user)
        raise SASLOperationError::SASL_OPS_USER_ALREADY_EXISTS
      end
      ret = `echo '#{password}' | saslpasswd2 -a memcached -c #{user} -p`

      if ret == ''
        return true
      end

      raise SASLOperationError::SASL_OPS_UNKNOWN_ERROR
    end

    def delete_user(user)
      if user.nil? || user.empty?
        raise SASLOperationError::SASL_OPS_ILLEGAL_INPUT
      end

      ret = `saslpasswd2 -d #{user}`
    end
  end

  class ProvisionedService
    include DataMapper::Resource
    property :name,       String,   :key => true
    property :port,       Integer,  :unique => true
    property :user,       String,   :required => true
    property :password,   String,   :required => true
    property :plan,       Enum[:free], :required => true
    property :pid,        Integer

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

  attr_accessor :local_db

  def initialize(options)
    super(options)

    @logger.warn("local_ip: #{@local_ip}")
    @sasl_admin = SASLAdmin.new(@logger)
    @base_dir = options[:base_dir]
    FileUtils.mkdir_p(@base_dir)
    @memcached_server_path = options[:memcached_server_path]
    @available_capacity = options[:capacity]
    @local_db = options[:local_db]
    @free_ports = Set.new
    @free_ports_mutex = Mutex.new
    options[:port_range].each {|port| @free_ports << port}
    @memcached_log_dir = options[:memcached_log_dir]
    @max_clients = @options[:max_clients] || 500
    @memcached_timeout = @options[:memcached_timeout] || 2
    @memcached_memory = @options[:memcached_memory]
    @sasl_enabled = @options[:sasl_enabled] || false
    @run_as_user =  @options[:run_as_user] || ""
    @supported_versions =["1.4"]
  end

  def pre_send_announcement
    super
    start_db
    start_provisioned_instances
  end

  def start_db
    DataMapper.setup(:default, @local_db)
    DataMapper::auto_upgrade!
  end

  def shutdown
    super
    ProvisionedService.all.each do |instance|
      stop_memcached_server(instance)
    end
    true
  end

  def announcement
    @capacity_lock.synchronize do
      a = {
          :available_capacity => @capacity,
          :capacity_unit => capacity_unit
      }
    end
  end

  def provision(plan, credentials = nil, version=nil)
    instance = ProvisionedService.new
    instance.plan = plan
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
      instance.user = credentials["user"]
      instance.password = credentials["password"]
    else
      @free_ports_mutex.synchronize do
        port = @free_ports.first
        @free_ports.delete(port)
        instance.port = port
      end
      instance.name = UUIDTools::UUID.random_create.to_s
      instance.user = UUIDTools::UUID.random_create.to_s
      instance.password = UUIDTools::UUID.random_create.to_s
    end

    begin
      instance.pid = start_instance(instance)
      @sasl_admin.create_user(instance.user, instance.password) if @sasl_enabled
      save_instance(instance)
      @logger.debug("Started process #{instance.pid}")
    rescue => e1
      begin
        cleanup_instance(instance)
      rescue => e2
        # Ignore the rollback exception
      end
      raise e1
    end

    # Sleep 1 second to wait for memcached instance start
    sleep 1
    gen_credentials(instance)
  end

  def unprovision(instance_id, credentials_list = [])
    instance = get_instance(instance_id)
    @logger.info("unprovision instance: #{instance.to_s}")
    cleanup_instance(instance)
    {}
  end

  def bind(instance_id, binding_options = :all, credentials = nil)
    # Memcached has no user level security, just return provisioned credentials.
    instance = nil
    if credentials
      instance = get_instance(credentials["name"])
    else
      instance = get_instance(instance_id)
    end
    gen_credentials(instance)
  end

  def unbind(credentials)
    # Memcached has no user level security, so has no operation for unbinding.
    {}
  end

  def restore(instance_id, backup_dir)
    # No restore command for memcached
    raise MemcachedError.new(MemcachedError::MEMCACHED_RESTORE_FILE_NOT_FOUND, dump_file)
    {}
  end

  def varz_details
    varz = {}
    varz[:provisioned_instances] = []
    varz[:provisioned_instances_num] = 0
    @capacity_lock.synchronize do
      varz[:max_instances_num] = @options[:capacity] / capacity_unit
    end
    ProvisionedService.all.each do |instance|
      varz[:provisioned_instances] << get_varz(instance)
      varz[:provisioned_instances_num] += 1
    end
    varz
  rescue => e
    @logger.warn("Error while getting varz details: #{e}")
    {}
  end

  def start_provisioned_instances
    @logger.debug("Start provisioned instance....")

    ProvisionedService.all.each do |instance|
      @capacity -= capacity_unit
      @logger.debug("instance : #{instance.inspect}")
      @free_ports_mutex.synchronize do
        @free_ports.delete(instance.port)
      end
      if instance.listening?
        @logger.warn("Service #{instance.name} already running on port #{instance.port}")
        next
      end
      begin
        pid = start_instance(instance)
        instance.pid = pid
        @logger.debug("Started Instace. pid is  #{instance.pid}")
        @sasl_admin.create_user(instance.user, instance.password) if @sasl_enabled
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

    @logger.debug("Started provisined instances.")
  end

  def save_instance(instance)
    raise MemcachedError.new(MemcachedError::MEMCACHED_SAVE_INSTANCE_FAILED, instance.inspect) unless instance.save
  end

  def destroy_instance(instance)
    raise MemcachedError.new(MemcachedError::MEMCACHED_DESTROY_INSTANCE_FAILED, instance.inspect) unless instance.destroy
  end

  def get_instance(name)
    instance = ProvisionedService.get(name)
    raise MemcachedError.new(MemcachedError::MEMCACHED_FIND_INSTANCE_FAILED, name) if instance.nil?
    instance
  end

  def build_option_string(opt)
    # ./memcached -m memory_size -p port_num -c connection -P pid_file -t -v -S
    str = ''
    str << " -m #{opt['memory']}"
    str << " -p #{opt['port']}"
    str << " -c #{opt['maxclients']}"
    str << " -v"
    str << " -S" if @sasl_enabled

    return str
  end

  def start_instance(instance)
    @logger.debug("Starting: #{instance.inspect}")

    opt = {}
    opt['memory'] = @memcached_memory
    opt['port'] = instance.port
    opt['password'] = instance.password
    opt['name'] = instance.name
    opt['maxclients'] = @max_clients

    option_string = build_option_string(opt)

    log_dir = instance_log_dir(instance.name)
    log_file = File.join(log_dir, "memcached.log")
    err_file = File.join(log_dir, "memcached.err.log")

    FileUtils.mkdir_p(log_dir)

    run_as_cmd_prefix = @run_as_user.empty? ? "" : "sudo -u #{@run_as_user}"
    cmd = "#{run_as_cmd_prefix} #{@memcached_server_path} #{option_string}"
    @logger.info("Executing CMD =  #{cmd}")

    pid = Process.spawn(cmd, :out=>"#{log_file}", :err=>"#{err_file}")
    Process.detach(pid)
    return pid
  rescue => e
    raise MemcachedError.new(MemcachedError::MEMCACHED_START_INSTANCE_FAILED, instance.inspect)
  end

  def stop_instance(instance)
    @logger.debug("Stop instance: #{instance.inspect}")
    stop_memcached_server(instance)
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
      @sasl_admin.delete_user(instance.user) if @sasl_enabled
    rescue => e
      err_msg << e.message
    end
    raise MemcachedError.new(MemcachedError::MEMCACHED_CLEANUP_INSTANCE_FAILED, err_msg.inspect) if err_msg.size > 0
  end

  def stop_memcached_server(instance)
     @logger.debug("stop process #{instance.pid}")

    Timeout::timeout(@memcached_timeout) do
      Process.kill("KILL", instance.pid.to_i)
    end
  rescue Timeout::Error => e
    @logger.warn(e)
  rescue => e
    @logger.warn(e)
  end

  def get_info(instance)
    user = instance.user
    password = instance.password
    hostname = 'localhost:' + instance.port.to_s
    info = nil
    Timeout::timeout(@memcached_timeout) do
      memcached = Dalli::Client.new(hostname, username: user, password: password)
      info = memcached.stats
    end
  rescue => e
    raise MemcachedError.new(MemcachedError::MEMCACHED_CONNECT_INSTANCE_FAILED)
  ensure
    begin
      memcached.close if memcached
      return info[info.keys.first]
    rescue => e
    end
  end

  def get_varz(instance)
    info = get_info(instance)
    varz = {}
    varz[:name] = instance.name
    varz[:port] = instance.port
    varz[:plan] = instance.plan
    varz[:usage] = {}
    varz[:usage][:capacity_unit] = capacity_unit
    varz[:usage][:max_clients] = @max_clients

    varz[:info] = info
    varz
  end

  def gen_credentials(instance)
    @logger.warn("local_ip: #{@local_ip}")
    credentials = {
      "hostname" => @local_ip,
      "host" => @local_ip,
      "port" => instance.port,
      "user" => instance.user,
      "password" => instance.password,
      "name" => instance.name
    }
  end


  def instance_dir(instance_id)
    File.join(@base_dir, instance_id)
  end

  def instance_log_dir(instance_id)
    File.join(@memcached_log_dir, instance_id)
  end
end
