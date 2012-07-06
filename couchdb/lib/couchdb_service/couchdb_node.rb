# Copyright (c) 2009-2011 VMware, Inc.
require "fileutils"

require "datamapper"
require "nats/client"
require "uuidtools"

require 'vcap/common'
require 'vcap/component'
require "couchdb_service/common"
require "couchdb_service/restapi"
require "couchdb_service/util"

require 'net/http'

require 'open3'

module VCAP
  module Services
    module CouchDB
      class Node < VCAP::Services::Base::Node
      end
    end
  end
end

require "couchdb_service/couchdb_error"

class VCAP::Services::CouchDB::Node

  include VCAP::Services::CouchDB::Common
  include VCAP::Services::CouchDB::RestAPI
  include VCAP::Services::CouchDB::Util
  include VCAP::Services::CouchDB

  # FIXME only support rw currently
  BIND_OPT = 'rw'

  class ProvisionedService
    include DataMapper::Resource
    property :name,       String,   :key => true
    property :port,       Integer
    property :user,       String,   :required => true
    property :password,   String,   :required => true
    property :plan,       Enum[:free], :required => true
  end

  def initialize(options)
    super(options)

    @couchdb_config = options[:couchdb]
    @logger.info("couchdb_config = #{@couchdb_config}")
    @couchdb_bind_host_ip = @couchdb_config['host']

    @logger.info("Bound to IP: #{@couchdb_bind_host_ip}")
    @couchdb_install_path = options[:couchdb_install_path]

    @base_dir = options[:base_dir]
    FileUtils.mkdir_p(@base_dir)

    DataMapper.setup(:default, options[:local_db])
    DataMapper::auto_upgrade!

    @available_capacity = options[:capacity]
    ProvisionedService.all.each do |provisioned_service|
      @capacity -= capacity_unit
    end

    @couchdb_ctl = "#{@couchdb_install_path}/etc/init.d/couchdb"

    # TODO: Investigate if there is a better way than this
    # Start couchdb process if its not already started
    @logger.info(`#{@couchdb_ctl} start`) if !couchdb_running?

    @supported_versions = ["1.2"]
  end

  def couchdb_running?
    couch_status_cmd = "#{@couchdb_ctl} status"
    stdin,stdout,stderr = Open3.popen3(couch_status_cmd)
    status_out = stdout.gets
    status_err = stderr.gets
    status_out = status_out.chomp if status_out != nil
    status_err = status_err.chomp if status_err != nil
    @logger.info("CouchDB status: \n\tOUT: #{status_out}\n\tERR: #{status_err}")
    false if status_err =~ /not running/
    true if status_out =~ /is running/
  end

  def shutdown
    super
    @logger.info("Shutting down couchdb...")
    @logger.info(`#{@couchdb_ctl} stop`) if couchdb_running?
  end

  def announcement
    @capacity_lock.synchronize do
      { :available_capacity => @capacity,
        :capacity_unit => capacity_unit }
    end
  end

  def provision(plan, credential = nil, version=nil)
    raise CouchDbError.new(CouchDbError::COUCHDB_INVALID_PLAN, plan) unless plan.to_s == @plan
    provisioned_service = ProvisionedService.new
    if credential
      name, user, password = %w(name user password).map{|key| credential[key]}
      provisioned_service.name = name
      provisioned_service.user = user
      provisioned_service.password = password
    else
      provisioned_service.name = 'd' + UUIDTools::UUID.random_create.to_s
      provisioned_service.user = 'u' + UUIDTools::UUID.random_create.to_s
      provisioned_service.password = 'p' + UUIDTools::UUID.random_create.to_s
    end
    provisioned_service.port      = @couchdb_config["port"]
    provisioned_service.plan      = plan

    unless provisioned_service.save
      raise CouchDbError.new(CouchDbError::COUCHDB_SAVE_INSTANCE_FAILED, provisioned_service.inspect)
    end

    begin
      couchdb_add_db(provisioned_service)
      couchdb_add_database_user(provisioned_service)
    rescue => e
      cleanup_service(provisioned_service)
      raise e.to_s + ": Could not save admin user."
    end

    response = {
      "hostname" => @couchdb_bind_host_ip,
      "host" => @couchdb_bind_host_ip,
      "port" => provisioned_service.port,
      "name" => provisioned_service.name,
      "username" => provisioned_service.user,
      "password" => provisioned_service.password
    }
    @logger.debug("response: #{response}")
    return response
  end

  def unprovision(name, bindings)
    provisioned_service = ProvisionedService.get(name)
    raise ServiceError.new(ServiceError::NOT_FOUND, name) if provisioned_service.nil?

    cleanup_service(provisioned_service)
    @logger.debug("Successfully fulfilled unprovision request: #{name}.")
    true
  end

  def cleanup_service(provisioned_service)
    raise CouchDbError.new(CouchDbError::COUCHDB_CLEANUP_INSTANCE_FAILED, provisioned_service.errors.inspect) unless provisioned_service.destroy

    couchdb_delete_database_user(provisioned_service)
    couchdb_flush_bound_users(provisioned_service)
    couchdb_delete_db(provisioned_service)

    true
  end

  def bind(name, bind_opts, credential = nil)
    @logger.debug("Bind request: name=#{name}, bind_opts=#{bind_opts}")
    bind_opts ||= BIND_OPT

    provisioned_service = ProvisionedService.get(name)
    raise ServiceError.new(ServiceError::NOT_FOUND, name) if provisioned_service.nil?

    username = credential && credential['username'] ? credential['username'] : UUIDTools::UUID.random_create.to_s
    password = credential && credential['password'] ? credential['password'] : UUIDTools::UUID.random_create.to_s

    couchdb_add_database_user(provisioned_service, username, password)

    response = {
      "hostname" => @couchdb_bind_host_ip,
      "host" => @couchdb_bind_host_ip,
      "port"     => provisioned_service.port,
      "username" => username,
      "password" => password,
      "name"     => provisioned_service.name
    }

    @logger.debug("response: #{response}")
    response
  end

  def unbind(credential)
    @logger.debug("Unbind request: credential=#{credential}")

    name = credential['name']
    provisioned_service = ProvisionedService.get(name)
    raise ServiceError.new(ServiceError::NOT_FOUND, name) if provisioned_service.nil?

    couchdb_delete_database_user(provisioned_service, credential['username'])

    @logger.debug("Successfully unbind #{credential}")
    true
  end

  def varz_details
    # Do disk summary
    du_hash = {}
    du_all_out = `cd #{@base_dir}; du -sk * 2> /dev/null`
    du_entries = du_all_out.split("\n")
    du_entries.each do |du_entry|
      size, dir = du_entry.split("\t")
      size = size.to_i * 1024 # Convert to bytes
      du_hash[dir] = size
    end

    stats = ProvisionedService.all.map do |provisioned_service|
      stat = {
        'name' => provisioned_service.name,
        'overall' => couchdb_overall_stats(provisioned_service),
        'db' => couchdb_db_stats(provisioned_service)
      }
    end

    {
      :running_services     => stats,
      :disk                 => du_hash,
    }
  end

  VALID_SALT_CHARACTERS = ("A".."Z").to_a + ("a".."z").to_a + ("0".."9").to_a
  def generate_salt(length = 12)
    Array.new(length) { VALID_SALT_CHARACTERS[rand(VALID_SALT_CHARACTERS.length)] }.join
  end
end
