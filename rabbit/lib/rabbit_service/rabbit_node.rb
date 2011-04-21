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

  class ProvisionedService
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
    @logger.info("Starting rabbit service node...")
    start_db
    start_server
  end

	def start_db
    DataMapper.setup(:default, @local_db)
    DataMapper::auto_upgrade!
	end

	def announcement
		a = {
			:available_memory => @available_memory
		}
	end

  def provision(plan)
    service = ProvisionedService.new
    service.name = "rabbit-#{UUIDTools::UUID.random_create.to_s}"
    service.plan = plan
    service.plan_option = ""
    service.vhost = "v" + UUIDTools::UUID.random_create.to_s.gsub(/-/, "")
    service.admin_username = "au" + generate_credential
    service.admin_password = "ap" + generate_credential
    service.memory   = @max_memory

		@available_memory -= service.memory

		save_service(service)

    add_vhost(service.vhost)
    add_user(service.admin_username, service.admin_password)
    set_permissions(service.vhost, service.admin_username, '".*" ".*" ".*"')

    credentials = {
			"name" => service.name,
			"hostname" => @local_ip,
      "port"  => @rabbit_port,
			"vhost" => service.vhost,
			"user" => service.admin_username,
			"pass" => service.admin_password
    }
  rescue => e
    # Rollback
      begin
        @available_memory += service.memory
        destroy_service(service)
        delete_vhost(service.vhost)
        delete_user(service.admin_username)
      rescue => e
        # Ignore the exception here
      end
    raise e
  end

  def unprovision(service_id, credentials_list = [])
    service = get_service(service_id)
		# Delete all bindings in this service
		credentials_list.each do |credentials|
		  unbind(credentials)
		end
    delete_user(service.admin_username)
    delete_vhost(service.vhost)
		destroy_service(service)
    @available_memory += service.memory
    {}
  end

  def bind(service_id, binding_options = :all)
	  credentials = {}
		service = get_service(service_id)
		credentials["hostname"] = @local_ip
    credentials["port"] = @rabbit_port
		credentials["user"] = "u" + generate_credential
		credentials["pass"] = "p" + generate_credential
		credentials["vhost"] = service.vhost
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

	def save_service(service)
		raise RabbitError.new(RabbitError::RABBIT_SAVE_SERVICE_FAILED, service.pretty_inspect) unless service.save
	end

	def destroy_service(service)
		raise RabbitError.new(RabbitError::RABBIT_DESTORY_SERVICE_FAILED, service.pretty_inspect) unless service.destroy
	end

	def get_service(service_id)
    service = ProvisionedService.get(service_id)
		raise RabbitError.new(RabbitError::RABBIT_FIND_SERVICE_FAILED, service_id) if service.nil?
		service
	end

  def start_server
    raise RabbitError.new(RabbitError::RABBIT_START_SERVER_FAILED) unless %x[#{@rabbit_server} -detached] == "Activating RabbitMQ plugins ...\n0 plugins activated:\n\n"
    sleep 1
    # If the guest user is existed, then delete it for security
    begin
      users.each do |user|
        if user == "guest"
          delete_user("guest")
          break
        end
      end
    rescue => e
    end
    true
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
    raise RabbitError.new(RabbitError::RABBIT_SET_PERMISSION_FAILEDi, username, permissions) unless %x[#{@rabbit_ctl} set_permissions -p #{vhost} #{username} #{permissions}].split(/\n/)[-1] == "...done."
  end

  def list_users
    data = %x[#{@rabbit_ctl} add_user #{username} #{password}]
    lines = data.split(/\n/)
    raise RabbitError.new(RabbitError::RABBIT_LIST_USER_FAILED) unless lines[-1] == "...done."
    users = []
    lines.each do |line|
      items = line.split(/\t/)
      if items.size > 1
        users.push(items[0])
      end
    end
    users
  end

  def generate_credential(length = 12)
    Array.new(length) {VALID_CREDENTIAL_CHARACTERS[rand(VALID_CREDENTIAL_CHARACTERS.length)]}.join
  end
end
