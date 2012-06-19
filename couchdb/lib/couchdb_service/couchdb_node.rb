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

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..', 'base', 'lib')
require 'base/node'

module VCAP
  module Services
    module CouchDB
      class Node < VCAP::Services::Base::Node
      end
    end
  end
end

class VCAP::Services::CouchDB::Node

  include VCAP::Services::CouchDB::Common
  include VCAP::Services::CouchDB::RestAPI
  include VCAP::Services::CouchDB::Util

  # FIXME only support rw currently
  BIND_OPT = 'rw'

  class ProvisionedService
    include DataMapper::Resource
    property :name,       String,   :key => true
    property :port,       Integer
    property :user,       String,   :required => true
    property :password,   String,   :required => true
    property :plan,       Enum[:free], :required => true
    property :memory,     Integer
  end

  def initialize(options)
    super(options)

    @couchdb_config = options[:couchdb]

    @base_dir = options[:base_dir]
    FileUtils.mkdir_p(@base_dir)

    DataMapper.setup(:default, options[:local_db])
    DataMapper::auto_upgrade!

    @total_memory = options[:available_memory]
    @available_memory = options[:available_memory]
    ProvisionedService.all.each do |provisioned_service|
      @available_memory -= (provisioned_service.memory || @max_memory)
    end
    @max_memory = options[:max_memory]
  end

  def announcement
    {
      :available_memory => @available_memory
    }
  end

  def provision(plan, credential = nil)
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
    provisioned_service.memory    = @max_memory

    unless provisioned_service.save
      raise "Could not save entry: #{provisioned_service.errors.inspect}"
    end

    begin
      couchdb_add_db(provisioned_service)
      couchdb_add_database_user(provisioned_service)
    rescue => e
      record_service_log(provisioned_service.name)
      cleanup_service(provisioned_service)
      raise e.to_s + ": Could not save admin user."
    end

    @available_memory -= (provisioned_service.memory || @max_memory)
    response = {
      "hostname" => @local_ip,
      "host" => @local_ip,
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
    raise "Could not cleanup service: #{provisioned_service.errors.inspect}" unless provisioned_service.destroy

    couchdb_delete_database_user(provisioned_service)
    couchdb_flush_bound_users(provisioned_service)
    couchdb_delete_db(provisioned_service)

    @available_memory += provisioned_service.memory

    true
  end

  def bind(name, bind_opts, credential = nil)
    @logger.debug("Bind request: name=#{name}, bind_opts=#{bind_opts}")
    bind_opts ||= BIND_OPT

    provisioned_service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if provisioned_service.nil?

    username = credential && credential['username'] ? credential['username'] : UUIDTools::UUID.random_create.to_s
    password = credential && credential['password'] ? credential['password'] : UUIDTools::UUID.random_create.to_s

    couchdb_add_database_user(provisioned_service, username, password)

    response = {
      "hostname" => @local_ip,
      "host" => @local_ip,
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
      :services_max_memory  => @total_memory,
      :services_used_memory => @total_memory - @available_memory
    }
  end

  def healthz_details
    healthz = {}
    healthz[:self] = "ok"
    ProvisionedService.all.each do |provisioned_service|
      healthz[provisioned_service.name.to_sym] = get_healthz(provisioned_service)
    end
    healthz
  rescue => e
    @logger.warn("Error get healthz details: #{e}")
    {:self => "fail"}
  end

  def memory_for_service(provisioned_service)
    case provisioned_service.plan
      when :free then @max_memory
      else
        raise "Invalid plan: #{provisioned_service.plan}"
    end
  end

  def service_dir(service_id)
    File.join(@base_dir, service_id)
  end

  def dump_file(to_dir)
    File.join(to_dir, 'dump_file')
  end

  def log_file(base_dir)
    File.join(base_dir, 'log')
  end

  def rm_lockfile(service_id)
    lockfile = File.join(service_dir(service_id), 'data', 'couchdb.lock')
    FileUtils.rm_rf(lockfile)
  end

  def record_service_log(service_id)
    @logger.warn(" *** BEGIN couchdb log - instance: #{service_id}")
    @logger.warn("")
    base_dir = service_dir(service_id)
    file = File.new(log_file(base_dir), 'r')
    while (line = file.gets)
      @logger.warn(line.chomp!)
    end
  rescue => e
    @logger.warn(e)
  ensure
    @logger.warn(" *** END couchdb log - instance: #{service_id}")
    @logger.warn("")
  end

  VALID_SALT_CHARACTERS = ("A".."Z").to_a + ("a".."z").to_a + ("0".."9").to_a
  def generate_salt(length = 12)
    Array.new(length) { VALID_SALT_CHARACTERS[rand(VALID_SALT_CHARACTERS.length)] }.join
  end
end
