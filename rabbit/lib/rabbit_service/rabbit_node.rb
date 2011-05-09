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
    @rabbit_ctl = options[:rabbit_ctl]
    @rabbit_server = options[:rabbit_server]
    @available_memory = options[:available_memory]
    @max_memory = options[:max_memory]
    @local_db = options[:local_db]
    @binding_options = ["configure", "write", "read"]
    @options = options
    @base_dir = options[:base_dir]
    FileUtils.mkdir_p(@base_dir) if @base_dir
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

  def provision(plan)
    instance = ProvisionedInstance.new
    instance.name = "rabbitmq-#{UUIDTools::UUID.random_create.to_s}"
    instance.plan = plan
    instance.plan_option = ""
    instance.vhost = "v" + UUIDTools::UUID.random_create.to_s.gsub(/-/, "")
    instance.admin_username = "au" + generate_credential
    instance.admin_password = "ap" + generate_credential
    instance.memory   = @max_memory

    @available_memory -= instance.memory

    save_instance(instance)

    add_vhost(instance.vhost)
    add_user(instance.admin_username, instance.admin_password)
    set_permissions(instance.vhost, instance.admin_username, '".*" ".*" ".*"')

    credentials = {
      "name" => instance.name,
      "hostname" => @local_ip,
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

  def bind(instance_id, binding_options = :all)
    credentials = {}
    instance = get_instance(instance_id)
    credentials["hostname"] = @local_ip
    credentials["port"] = @rabbit_port
    credentials["user"] = "u" + generate_credential
    credentials["pass"] = "p" + generate_credential
    credentials["vhost"] = instance.vhost
    add_user(credentials["user"], credentials["pass"])
    set_permissions(credentials["vhost"], credentials["user"], get_permissions(binding_options))

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
    raise RabbitError.new(RabbitError::RABBIT_START_SERVER_FAILED) unless %x[#{@rabbit_server} -detached] == "Activating RabbitMQ plugins ...\n0 plugins activated:\n\n"
    sleep 1
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
    true
  end

  def stop_server
    raise RabbitError.new(RabbitError::RABBIT_STOP_SERVER_FAILED) unless %x[#{@rabbit_ctl} stop].split(/\n/)[-1] == "...done."
  end

  def add_vhost(vhost)
    raise RabbitError.new(RabbitError::RABBIT_ADD_VHOST_FAILED, vhost) unless %x[#{@rabbit_ctl} add_vhost #{vhost}].split(/\n/)[-1] == "...done."
  end

  def delete_vhost(vhost)
    raise RabbitError.new(RabbitError::RABBIT_DELETE_VHOST_FAILED, vhost) unless %x[#{@rabbit_ctl} delete_vhost #{vhost}].split(/\n/)[-1] == "...done."
  end

  def add_user(username, password)
    raise RabbitError.new(RabbitError::RABBIT_ADD_USER_FAILED, username) unless %x[#{@rabbit_ctl} add_user #{username} #{password}].split(/\n/)[-1] == "...done."
  end

  def delete_user(username)
    raise RabbitError.new(RabbitError::RABBIT_DELETE_USER_FAILED, username) unless %x[#{@rabbit_ctl} delete_user #{username}].split(/\n/)[-1] == "...done."
  end

  def get_permissions(binding_options)
    '".*" ".*" ".*"'
  end

  def set_permissions(vhost, username, permissions)
    raise RabbitError.new(RabbitError::RABBIT_SET_PERMISSION_FAILED, username, permissions) unless %x[#{@rabbit_ctl} set_permissions -p #{vhost} #{username} #{permissions}].split(/\n/)[-1] == "...done."
  end

  def list_users
    data = %x[#{@rabbit_ctl} list_users]
    lines = data.split(/\n/)
    raise RabbitError.new(RabbitError::RABBIT_LIST_USERS_FAILED) unless lines[-1] == "...done."
    users = []
    lines.each do |line|
      items = line.split(/\t/)
      if items.size > 1
        users << items[0]
      end
    end
    users
  end

  def list_queues(vhost)
    data = %x[#{@rabbit_ctl} list_queues -p #{vhost}]
    lines = data.split(/\n/)
    raise RabbitError.new(RabbitError::RABBIT_LIST_QUEUES_FAILED) unless lines[-1] == "...done."
    queues = []
    lines.each do |line|
      items = line.split(/\t/)
      if items.size > 1
        queues << items[0]
      end
    end
    queues
  end

  def list_exchanges(vhost)
    data = %x[#{@rabbit_ctl} list_exchanges -p #{vhost}]
    lines = data.split(/\n/)
    raise RabbitError.new(RabbitError::RABBIT_LIST_EXCHANGES_FAILED) unless lines[-1] == "...done."
    exchanges = []
    lines.each do |line|
      items = line.split(/\t/)
      if items.size > 1
        exchanges << items[0]
      end
    end
    exchanges
  end

  def list_bindings(vhost)
    data = %x[#{@rabbit_ctl} list_bindings -p #{vhost}]
    lines = data.split(/\n/)
    raise RabbitError.new(RabbitError::RABBIT_LIST_BINDINGS_FAILED) unless lines[-1] == "...done."
    bindings = []
    lines.each do |line|
      items = line.split(/\t/)
      if items.size > 1
        bindings.push << items[0]
      end
    end
    bindings
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
