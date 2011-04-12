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

VALID_CREDENTIAL_CHARACTERS = ("A".."Z").to_a + ("a".."z").to_a + ("0".."9").to_a

class VCAP::Services::Rabbit::Node

	include VCAP::Services::Rabbit::Common

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

		save_provisioned_service(service)

    add_vhost(service.vhost)
    add_user(service.admin_username, service.admin_password)
    set_permissions(service.vhost, service.admin_username, '".*" ".*" ".*"')

    response = {
			"name" => service.name,
			"hostname" => @local_ip,
      "port"  => @rabbit_port,
			"vhost" => service.vhost,
			"user" => service.admin_username,
			"pass" => service.admin_password
    }
  rescue => e
		@available_memory += service.memory
    @logger.warn(e)
		nil
  end

  def unprovision(service_id, handles = {})
    service = get_provisioned_service(service_id)
		# Delete all bindings in this service
		handles.each do |handle|
		  unbind(handle)
		end
    delete_user(service.admin_username)
    delete_vhost(service.vhost)
		destroy_provisioned_service(service)
    @available_memory += service.memory
		true
  rescue => e
    @logger.warn(e)
		nil
  end

  def bind(service_id, binding_options = :all)
	  handle = {}
		service = get_provisioned_service(service_id)
		handle["hostname"] = @local_ip
    handle["port"] = @rabbit_port
		handle["user"] = "u" + generate_credential
		handle["pass"] = "p" + generate_credential
		handle["vhost"] = service.vhost
		add_user(handle["user"], handle["pass"])
		set_permissions(handle["vhost"], handle["user"], get_permissions(binding_options))

		handle
  rescue => e
    @logger.warn(e)
		nil
  end

  def unbind(handle)
    delete_user(handle["user"])
		true
  rescue => e
    @logger.warn(e)
		nil
  end

	def save_provisioned_service(provisioned_service)
		raise "Could not save service: #{provisioned_service.errors.pretty_inspect}" unless provisioned_service.save
	end

	def destroy_provisioned_service(provisioned_service)
    raise "Could not delete service: #{provisioned_service.errors.pretty_inspect}" unless provisioned_service.destroy
	end

	def get_provisioned_service(service_id)
    provisioned_service = ProvisionedService.get(service_id)
		raise "Could not find service: #{service_id}" if provisioned_service.nil?
		provisioned_service
	end

  def start_server
    %x[#{@rabbit_server} -detached]
  end

  def add_vhost(vhost)
    %x[#{@rabbit_ctl} add_vhost #{vhost}]
  end

  def delete_vhost(vhost)
    %x[#{@rabbit_ctl} delete_vhost #{vhost}]
  end

  def add_user(username, password)
    %x[#{@rabbit_ctl} add_user #{username} #{password}]
  end

  def delete_user(username)
    %x[#{@rabbit_ctl} delete_user #{username}]
  end

	def get_permissions(binding_options)
	  '".*" ".*" ".*"'
	end

  def set_permissions(vhost, username, permissions)
    %x[#{@rabbit_ctl} set_permissions -p #{vhost} #{username} #{permissions}]
  end

  def generate_credential(length = 12)
    Array.new(length) {VALID_CREDENTIAL_CHARACTERS[rand(VALID_CREDENTIAL_CHARACTERS.length)]}.join
  end
end
