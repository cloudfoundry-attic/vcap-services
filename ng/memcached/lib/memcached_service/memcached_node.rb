# Copyright (c) 2009-2011 VMware, Inc.
# This code is based on Redis as a Service.

require "erb"
require "fileutils"
require "logger"

require 'socket'

require "uuidtools"
require 'dalli'
require "thread"

require "nats/client"
require "warden/client"
require 'vcap/common'
require 'vcap/component'

require "memcached_service/common"
require "memcached_service/memcached_error"

module VCAP
  module Services
    module Memcached
      class Node < VCAP::Services::Base::Node
        class ProvisionedService < VCAP::Services::Base::Warden::Service
        end
      end
    end
  end
end

class VCAP::Services::Memcached::Node
  include VCAP::Services::Memcached
  include VCAP::Services::Memcached::Common
  include VCAP::Services::Base::Utils
  include VCAP::Services::Base::Warden::NodeUtils

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

  attr_accessor :local_db

  def initialize(options)
    super(options)

    ProvisionedService.init(options)

    @base_dir = options[:base_dir]
    @local_db = options[:local_db]

    @service_start_timeout = options[:service_start_timeout] || 3
    init_ports(options[:port_range])

    @sasl_admin = SASLAdmin.new(@logger)

    @default_version = "1.4"
    @supported_versions = ["1.4"]
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
    start_provisioned_instances
    warden_node_init(@options)
  end

  def service_instances
    ProvisionedService.all
  end

  def start_provisioned_instances
    start_all_instances
    @capacity_lock.synchronize{ @capacity -= ProvisionedService.all.size }
  end

  def shutdown
    super
    @logger.info("Shutting down instances..")
    stop_all_instances
  end

  def announcement
    @capacity_lock.synchronize do
      { :available_capacity => @capacity, :capacity_unit => capacity_unit }
    end
  end

  def provision(plan, credentials = nil, version=nil)
    @logger.info("Provision request: plan=#{plan}, version=#{version}")
    #raise MemcachedError.new(MemcachedError::MEMCACHED_INVALID_PLAN, plan) unless plan == @plan
    raise ServiceError.new(ServiceError::UNSUPPORTED_VERSION, version) unless @supported_versions.include?(version)

    credentials = {} if credentials.nil?
    provision_options = {}
    provision_options["plan"]     = plan
    provision_options["version"]  = version
    provision_options["name"]     = credentials["name"]     || UUIDTools::UUID.random_create.to_s
    provision_options["user"]     = credentials["user"]     || UUIDTools::UUID.random_create.to_s
    provision_options["password"] = credentials["password"] || UUIDTools::UUID.random_create.to_s
    provision_options["port"]     = new_port(credentials["port"] || nil)

    p_service = create_provisioned_instance(provision_options)

    @logger.info("Starting: #{p_service.inspect}")
    p_service.run do
      @sasl_admin.create_user(p_service.user, p_service.password) if @sasl_enabled
    end

    response = get_credentials(p_service)
    @logger.debug("Provision response: #{response}")
    response
  rescue => e
     @logger.error("Error provisioning instance: #{e}")
     p_service.delete unless p_service.nil?
     raise MemcachedError.new(MemcachedError::MEMCACHED_START_INSTANCE_FAILED, e)
  end

  def create_provisioned_instance(provision_options)
    ProvisionedService.create(provision_options)
  end

  def unprovision(name, bindings = [])
    p_service = ProvisionedService.get(name)
    raise ServiceError.new(ServiceError::NOT_FOUND, name) if p_service.nil?
    port = p_service.port
    raise "Could not cleanup instance #{name}" unless p_service.delete
    free_port(port);

    @sasl_admin.delete_user(p_service.user) if @sasl_enabled

    @logger.info("Successfully fulfilled unprovision request: #{name}.")
    true
  end

  def bind(name, bind_opts = nil, credential = nil)
    p_service = ProvisionedService.get(name)
    raise ServiceError.new(ServiceError::NOT_FOUND, name) if p_service.nil?

    # Memcached has no user level security, just return provisioned credentials.
    get_credentials(p_service)
  end

  def unbind(credentials)
    # Memcached has no user level security, so has no operation for unbinding.
    {}
  end

  def restore(instance_id, backup_dir)
    # No restore command for memcached
    {}
  end


  def get_credentials(p_service)
    host_ip = get_host
    credentials = {
      "name"     => p_service.name,
      "hostname" => host_ip,
      "host"     => host_ip,
      "port"     => p_service.port,
      "user"     => p_service.user,
      "password" => p_service.password
    }
  end

  def varz_details
    varz = {}
    varz[:provisioned_instances_num] = 0
    varz[:max_instances_num] = @options[:capacity] / capacity_unit

    varz[:provisioned_instances] = ProvisionedService.all.map do |p_service|
      stat = {}
      stat['name']    = p_service.name
      stat['version'] = p_service.version
      stat['plan']    = @plan
      stat['port']    = p_service.port
      stat['info']    = p_service.get_instance_stats

      varz[:provisioned_instances_num] += 1

      stat
    end
    varz
  rescue => e
    @logger.warn("Error while getting varz details: #{e}")
    {}
  end
end

class VCAP::Services::Memcached::Node::ProvisionedService
  include DataMapper::Resource
  include VCAP::Services::Memcached

  property :name,       String,      :key => true
  property :port,       Integer,     :unique => true
  property :user,       String,      :required => true
  property :password,   String,      :required => true
  # property plan is deprecated. The instances in one node have same plan.
  property :plan,       Integer,     :required => true
  property :version,    String,      :required => false

  property :container,  String
  property :ip,         String

  private_class_method :new

  SERVICE_PORT = 27017

  class << self
    def init(args)
      super(args)
      @@memcached_timeout     = args[:memcached_timeout] || 2
      @@memcached_memory      = args[:memcached_memory]
      @@max_clients           = args[:max_clients] || 500
      @@sasl_enabled          = args[:sasl_enabled] || false
    end

    def create(args)
      raise "Parameter missing" unless args['port']
      p_service           = new
      p_service.name      = args['name']
      p_service.port      = args['port']
      p_service.plan      = 1
      p_service.user      = args['user']
      p_service.password  = args['password']
      p_service.version   = args['version']

      p_service.prepare_filesystem(1)
      p_service
    end
  end

  def connect
    conn = nil
    return conn unless running?
    Timeout::timeout(@@memcached_timeout) do
      conn = Dalli::Client.new("#{self[:ip]}\:#{SERVICE_PORT}", username: self[:user], password: self[:password])
    end
  rescue => e
    logger.error("Failed to connect to instance: #{self[:ip]}\:#{SERVICE_PORT} - #{e.inspect}")
    raise MemcachedError.new(MemcachedError::MEMCACHED_CONNECT_INSTANCE_FAILED, @self[:ip])
  ensure
    return conn
  end

  def get_instance_stats
    logger.debug("Get Stats: memcached instance: #{self[:ip]}\:#{SERVICE_PORT}")
    return [] unless running?
    conn = connect
    info = nil
    Timeout::timeout(@@memcached_timeout) do
      info = conn.stats
    end
  rescue => e
    logger.error("Failed to get stats for instance: #{self[:ip]}\:#{SERVICE_PORT} - #{e.inspect}")
    raise e
  ensure
    begin
      conn.close if conn
      return info[info.keys.first]
    rescue => e
    end
  end

  def bin_dir
    self.class.bin_dir["memcached"]
  end

  def start_script
    # memcached -m memory_size -p port_num -c connection -v -S
    cmd_components = [
      "#{service_script}",
      "start",
      "#{base_dir}",
      "#{log_dir}",
      "#{bin_dir}",
      "-m #{@@memcached_memory}",
      "-p #{SERVICE_PORT}",
      "-c #{@@max_clients}",
      "-v",
      @@sasl_enabled ? "-S" : ""
    ]
    cmd_components.join(" ")
  end

  def start_options
    options = super
    options[:start_script] = {:script => start_script, :use_spawn => true}
    options[:service_port] = SERVICE_PORT
    options
  end

  def finish_first_start?
    conn = nil
    Timeout::timeout(@@memcached_timeout) do
      conn = Dalli::Client.new("#{self[:ip]}\:#{SERVICE_PORT}")
      conn.get("foobar")
      return true
    end
  rescue => e
    return false
  ensure
    begin
      conn.close if conn
    rescue => e
    end
  end

  def finish_start?
    # No user check is the same with first start check
    return finish_first_start? unless @@sasl_enabled
    conn = nil
    begin
      Timeout::timeout(@@memcached_timeout) do
        conn = Dalli::Client.new("#{self[:ip]}\:#{SERVICE_PORT}", username: self[:user], password: self[:password])
        conn.get("foobar")
      end
    rescue => e
      return false
    ensure
      begin
        conn.close if conn
      rescue => e
      end
    end
  end
end
