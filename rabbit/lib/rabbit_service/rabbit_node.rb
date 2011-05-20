# Copyright (c) 2009-2011 VMware, Inc.
require "set"
require "datamapper"
require "uuidtools"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..', 'base', 'lib')
require 'base/node'

module VCAP
  module Services
    module Rabbit
      class Node < VCAP::Services::Base::Node
      end
    end
  end
end

require "rabbit_service/common"
require "rabbit_service/rabbit_error"

VALID_CREDENTIAL_CHARACTERS = ("A".."Z").to_a + ("a".."z").to_a + ("0".."9").to_a

class VCAP::Services::Rabbit::Node

  include VCAP::Services::Rabbit::Common
  include VCAP::Services::Rabbit

  class ProvisionedInstance
    include DataMapper::Resource
    property :name,            String,      :key => true
    property :vhost,           String,      :required => true
    property :admin_username,  String,      :required => true
    property :admin_password,  String,      :required => true
    property :plan,            Enum[:free], :required => true
    property :plan_option,     String,      :required => false
    property :memory,          Integer,     :required => true
  end

  def initialize(options)
    super(options)
    @rabbit_port = options[:rabbit_port] || 5672
    ENV["RABBITMQ_NODE_PORT"] = @rabbit_port.to_s
    @rabbit_ctl = options[:rabbit_ctl]
    @rabbit_server = options[:rabbit_server]
    @available_memory = options[:available_memory]
    @max_memory = options[:max_memory]
    @local_db = options[:local_db]
    @binding_options = ["configure", "write", "read"]
    @base_dir = options[:base_dir]
    FileUtils.mkdir_p(@base_dir) if @base_dir
    @options = options
  end

  def start
    @logger.info("Starting rabbit node...")
    start_db
    start_server
    ProvisionedInstance.all.each do |instance|
      @available_memory -= (instance.memory || @max_memory)
    end
    true
  end

  def shutdown
    super if defined?(super)
    stop_server
    true
  end

  def announcement
    a = {
      :available_memory => @available_memory
    }
  end

  def provision(plan, credentials = nil)
    instance = ProvisionedInstance.new
    instance.plan = plan
    instance.plan_option = ""
    if credentials
      instance.name = credentials["name"]
      instance.vhost = credentials["vhost"]
      instance.admin_username = credentials["user"]
      instance.admin_password = credentials["pass"]
    else
      instance.name = "rabbitmq-#{UUIDTools::UUID.random_create.to_s}"
      instance.vhost = "v" + UUIDTools::UUID.random_create.to_s.gsub(/-/, "")
      instance.admin_username = "au" + generate_credential
      instance.admin_password = "ap" + generate_credential
    end
    instance.memory   = @max_memory

    @available_memory -= instance.memory

    save_instance(instance)

    add_vhost(instance.vhost)
    add_user(instance.admin_username, instance.admin_password)
    set_permissions(instance.vhost, instance.admin_username, "'.*' '.*' '.*'")

    credentials = {
      "name" => instance.name,
      "host" => @local_ip,
      "port"  => @rabbit_port,
      "vhost" => instance.vhost,
      "user" => instance.admin_username,
      "pass" => instance.admin_password
    }
  rescue => e
    # Rollback
      begin
        cleanup_instance(instance)
      rescue => e
        # Ignore the exception here
      end
    raise e
  end

  def unprovision(instance_id, credentials_list = [])
    instance = get_instance(instance_id)
    cleanup_instance(instance, credentials_list)
    {}
  end

  def bind(instance_id, binding_options = :all, binding_credentials = nil)
    credentials = {}
    instance = get_instance(instance_id)
    credentials["name"] = instance_id
    credentials["host"] = @local_ip
    credentials["port"] = @rabbit_port
    if binding_credentials
      credentials["vhost"] = binding_credentials["vhost"]
      credentials["user"] = binding_credentials["user"]
      credentials["pass"] = binding_credentials["pass"]
    else
      credentials["vhost"] = instance.vhost
      credentials["user"] = "u" + generate_credential
      credentials["pass"] = "p" + generate_credential
    end
    add_user(credentials["user"], credentials["pass"])
    set_permissions(credentials["vhost"], credentials["user"], get_permissions_by_options(binding_options))

    credentials
  rescue => e
    # Rollback
      begin
        delete_user(credentials["user"])
      rescue => e
        # Ignore the exception here
      end
    raise e
  end

  def unbind(credentials)
    delete_user(credentials["user"])
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
    logger.warn(e)
    {}
  end

  # Clean all users permissions
  def disable_instance(service_credentials, binding_credentials_list = [])
    clear_permissions(service_credentials["vhost"], service_credentials["user"])
    binding_credentials_list.each do |credentials|
      clear_permissions(credentials["vhost"], credentials["user"])
    end
    true
  end

  # This function may run in old node or new node, it does these things:
  # 1. Try to check user permissions
  # 2. If permissions are empty, then it's the new node, otherwise the old node.
  # 3. For new node, it need do binding using all binding credentials,
  #    for old node, it should restore all the users permissions.
  def enable_instance(service_credentials, binding_credentials_map = [])
    if get_permissions(service_credentials["vhost"], service_credentials["user"]) != ""
      # The new node
      binding_credentials_map.each do |key, value|
        bind(service_credentials["name"], value["binding_options"], value["credentials"])
      end
    else
      # The old node
      set_permissions(service_credentials["vhost"], service_credentials["user"], "'.*' '.*' '.*'")
      binding_credentials_map.each do |key, value|
        set_permissions(value["credentials"]["vhost"], value["credentials"]["user"], get_permissions_by_options(value["binding_options"]))
      end
    end
    [service_credentials, binding_credentials_map]
  rescue => e
    @logger.warn(e)
    nil
  end

  # Rabbitmq has no data to dump for migration
  def dump_instance(service_credentials, binding_credentials_list = [], dump_dir)
    true
  end

  def import_instance(service_credentials, binding_credentials_list = [], dump_dir, plan)
    provision(plan, service_credentials)
  end

  def start_db
    DataMapper.setup(:default, @local_db)
    DataMapper::auto_upgrade!
  end

  def save_instance(instance)
    raise RabbitError.new(RabbitError::RABBIT_SAVE_INSTANCE_FAILED, instance.pretty_inspect) unless instance.save
  end

  def destroy_instance(instance)
    raise RabbitError.new(RabbitError::RABBIT_DESTORY_INSTANCE_FAILED, instance.pretty_inspect) unless instance.destroy
  end

  def get_instance(instance_id)
    instance = ProvisionedInstance.get(instance_id)
    raise RabbitError.new(RabbitError::RABBIT_FIND_INSTANCE_FAILED, instance_id) if instance.nil?
    instance
  end

  def cleanup_instance(instance, credentials_list = [])
    err_msg = []
    @available_memory += instance.memory
    # Delete all bindings in this instance
    begin
      credentials_list.each do |credentials|
        unbind(credentials)
      end
    rescue => e
      err_msg << e.message
    end
    begin
      delete_vhost(instance.vhost)
    rescue => e
      err_msg << e.message
    end
    begin
      delete_user(instance.admin_username)
    rescue => e
      err_msg << e.message
    end
    begin
      destroy_instance(instance)
    rescue => e
      err_msg << e.message
    end
    raise RabbitError.new(RabbitError::RABBIT_CLEANUP_INSTANCE_FAILED, err_msg.inspect) if err_msg.size > 0
  end

  def start_server
    output = %x[#{@rabbit_server} -detached]
    if output == "Activating RabbitMQ plugins ...\n0 plugins activated:\n\n"
      sleep 2
      # If the guest user is existed, then delete it for security
      begin
        users = list_users
        users.each do |user|
          if user == "guest"
            delete_user(user)
            break
          end
        end
      rescue => e
      end
      return true
    else
      logger.warn("rabbitmqctl error: #{output}")
      raise RabbitError.new(RabbitError::RABBIT_START_SERVER_FAILED)
    end
  end

  def stop_server
    output = %x[#{@rabbit_ctl} stop 2>&1]
    if output.split(/\n/)[-1] == "...done."
      return true
    else
      logger.warn("rabbitmqctl error: #{output}")
      raise RabbitError.new(RabbitError::RABBIT_STOP_SERVER_FAILED)
    end
  end

  def add_vhost(vhost)
    output = %x[#{@rabbit_ctl} add_vhost #{vhost} 2>&1]
    if output.split(/\n/)[-1] == "...done."
      return true
    else
      logger.warn("rabbitmqctl error: #{output}")
      raise RabbitError.new(RabbitError::RABBIT_ADD_VHOST_FAILED, vhost)
    end
  end

  def delete_vhost(vhost)
    output = %x[#{@rabbit_ctl} delete_vhost #{vhost} 2>&1]
    if output.split(/\n/)[-1] == "...done."
      return true
    else
      logger.warn("rabbitmqctl error: #{output}")
      raise RabbitError.new(RabbitError::RABBIT_DELETE_VHOST_FAILED, vhost)
    end
  end

  def add_user(username, password)
    output = %x[#{@rabbit_ctl} add_user #{username} #{password} 2>&1]
    if output.split(/\n/)[-1] == "...done."
      return true
    else
      logger.warn("rabbitmqctl error: #{output}")
      raise RabbitError.new(RabbitError::RABBIT_ADD_USER_FAILED, username)
    end
  end

  def delete_user(username)
    output = %x[#{@rabbit_ctl} delete_user #{username} 2>&1]
    if output.split(/\n/)[-1] == "...done."
      return true
    else
      logger.warn("rabbitmqctl error: #{output}")
      raise RabbitError.new(RabbitError::RABBIT_DELETE_USER_FAILED, username)
    end
  end

  def get_permissions_by_options(binding_options)
    "'.*' '.*' '.*'"
  end

  def get_permissions(vhost, username)
    output = %x[#{@rabbit_ctl} list_user_permissions -p #{vhost} #{username} 2>&1]
    lines = output.split(/\n/)
    if lines[-1] == "...done."
      if lines.size == 3
        list = lines[1].split(/\t/)
        return "'#{list[1]}' '#{list[2]}' '#{list[3]}'"
      elsif lines.size == 2
        return ""
      else
       raise RabbitError.new(RabbitError::RABBIT_GET_PERMISSIONS_FAILED, username)
      end
    else
      logger.warn("rabbitmqctl error: #{output}")
      raise RabbitError.new(RabbitError::RABBIT_GET_PERMISSIONS_FAILED, username)
    end
  end

  def set_permissions(vhost, username, permissions)
    output = %x[#{@rabbit_ctl} set_permissions -p #{vhost} #{username} #{permissions} 2>&1]
    if output.split(/\n/)[-1] == "...done."
      return true
    else
      logger.warn("rabbitmqctl error: #{output}")
      raise RabbitError.new(RabbitError::RABBIT_SET_PERMISSIONS_FAILED, username, permissions)
    end
  end

  def clear_permissions(vhost, username)
    output = %x[#{@rabbit_ctl} clear_permissions -p #{vhost} #{username} 2>&1]
    if output.split(/\n/)[-1] == "...done."
      return true
    else
      logger.warn("rabbitmqctl error: #{output}")
      raise RabbitError.new(RabbitError::RABBIT_CLEAR_PERMISSIONS_FAILED, username)
    end
  end

  def list_users
    output = %x[#{@rabbit_ctl} list_users 2>&1]
    lines = output.split(/\n/)
    if lines[-1] == "...done."
      users = []
      lines.each do |line|
        items = line.split(/\t/)
        if items.size > 1
          users << items[0]
        end
      end
      return users
    else
      logger.warn("rabbitmqctl error: #{output}")
      raise RabbitError.new(RabbitError::RABBIT_LIST_USERS_FAILED)
    end
  end

  def list_queues(vhost)
    output = %x[#{@rabbit_ctl} list_queues -p #{vhost} 2>&1]
    lines = output.split(/\n/)
    if lines[-1] == "...done."
      queues = []
      lines.each do |line|
        items = line.split(/\t/)
        if items.size > 1
          queues << items[0]
        end
      end
      return queues
    else
      logger.warn("rabbitmqctl error: #{output}")
      raise RabbitError.new(RabbitError::RABBIT_LIST_USERS_FAILED, vhost)
    end
  end

  def list_exchanges(vhost)
    output = %x[#{@rabbit_ctl} list_exchanges -p #{vhost} 2>&1]
    lines = output.split(/\n/)
    if lines[-1] == "...done."
      exchanges = []
      lines.each do |line|
        items = line.split(/\t/)
        if items.size > 1
          exchanges << items[0]
        end
      end
      return exchanges
    else
      logger.warn("rabbitmqctl error: #{output}")
      raise RabbitError.new(RabbitError::RABBIT_LIST_EXCHANGES_FAILED, vhost)
    end
  end

  def list_bindings(vhost)
    output = %x[#{@rabbit_ctl} list_bindings -p #{vhost} 2>&1]
    lines = output.split(/\n/)
    if lines[-1] == "...done."
      bindings = []
      lines.each do |line|
        items = line.split(/\t/)
        if items.size > 1
          bindings.push << items[0]
        end
      end
      return bindings
    else
      logger.warn("rabbitmqctl error: #{output}")
      raise RabbitError.new(RabbitError::RABBIT_LIST_BINDINGS_FAILED, vhost)
    end
  end

  def generate_credential(length = 12)
    Array.new(length) {VALID_CREDENTIAL_CHARACTERS[rand(VALID_CREDENTIAL_CHARACTERS.length)]}.join
  end

  def get_varz(instance)
    varz = {}
    varz[:name] = instance.name
    varz[:plan] = instance.plan
    varz[:vhost] = instance.vhost
    varz[:admin_username] = instance.admin_username
    varz[:usage] = {}
    varz[:usage][:queues_num] = list_queues(instance.vhost).size
    varz[:usage][:exchanges_num] = list_exchanges(instance.vhost).size
    varz[:usage][:bindings_num] = list_bindings(instance.vhost).size
    varz
  end

end
