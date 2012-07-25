# Copyright (c) 2009-2011 VMware, Inc.

require "erb"
require "fileutils"
require "logger"
require "pp"
require "set"
require "timeout"
require "net/http"
require "openssl"
require "digest/sha2"
require "yajl"
require "json"
require "base64"

require "nats/client"
require "uuidtools"

require "vcap/common"
require "vcap/component"
require "vblob_service/common"
require "vblob_service/vblob_error"

require "sys/filesystem"
require "find"

include Sys

module VCAP
  module Services
    module VBlob
      class Node < VCAP::Services::Base::Node
        class ProvisionedService
        end
      end
    end
  end
end

class VCAP::Services::VBlob::Node

  BIND_OPT = 'rw'
  VBLOB_TIMEOUT = 1
  VBLOB_MAX_SYSTEM_COMMAND_RETRY = 5

  include VCAP::Services::VBlob::Common
  include VCAP::Services::Base::Utils
  include VCAP::Services::VBlob

  def initialize(options)
    super(options)
    @base_dir = options[:base_dir]
    init_ports(options[:port_range])
    @service_start_timeout = @options[:service_start_timeout] || 3
    ProvisionedService.init(options)
    @supported_versions = ["1.0"]
  end

  # handle the cases which has already been in the local sqlite database
  def pre_send_announcement
    @capacity_lock.synchronize do
      start_instances(ProvisionedService.all)
    end
  end

  def shutdown
    super
    @logger.info("Shutting down instances..")
    ProvisionedService.all.each do |provisioned_service|
      @logger.debug("Trying to terminate vblobd container: #{provisioned_service.name}")
      provisioned_service.stop if provisioned_service.running?
    end
  end

  def announcement
    @capacity_lock.synchronize do
      { :available_capacity => @capacity,
        :capacity_unit => capacity_unit }
    end
  end

  def all_instances_list
    ProvisionedService.all.map{|ps| ps["name"]}
  end

  def all_bindings_list
    list = []
    ProvisionedService.all.each do |provisioned_service|
      begin
        http = Net::HTTP.new(provisioned_service[:ip], provisioned_service.service_port)
        request = Net::HTTP::Get.new("/~bind")
        request.basic_auth(provisioned_service.keyid, provisioned_service.secretid)
        http.open_timeout = http.read_timeout = VBLOB_TIMEOUT
        response = http.request(request)
        raise "Couldn't get binding list" if (!response || response.code != "200")
        bindings = Yajl::Parser.parse(response.body)
        bindings.each_key {|key|
          credential = {
            'name' => provisioned_service.name,
            'port' => provisioned_service.port,
            'username' => key
          }
          list << credential if credential['username'] != provisioned_service.keyid
        }
      rescue => e
        @logger.warn("Failed to fetch user list: #{e.message}")
      end
    end
    list
  end

  # will be re-used by restore codes; thus credential could be none null
  def provision(plan, credential = nil, version=nil)
    @logger.debug("Provision a service instance")

    port     = credential && credential['port'] ? new_port(credential['port']) : new_port
    name     = credential && credential['name'] ? credential['name'] : UUIDTools::UUID.random_create.to_s
    username = credential && credential['username'] ? credential['username'] : UUIDTools::UUID.random_create.to_s
    password = credential && credential['password'] ? credential['password'] : UUIDTools::UUID.random_create.to_s

    provisioned_service = ProvisionedService.create(port, name, username, password)
    provisioned_service.run

    raise VBlobError.new(VBlobError::VBLOB_START_INSTANCE_ERROR) if wait_service_start(provisioned_service) == false

    host = get_host
    response = {
      "hostname" => host,
      "host" => host,
      "port" => provisioned_service.port,
      "name" => provisioned_service.name,
      "username" => username,
      "password" => password
    }
    @logger.debug("Provision response: #{response}")
    return response
  rescue => e
    @logger.error("Error provision instance: #{e}")
    provisioned_service.delete unless provisioned_service.nil?
    free_port(port) unless port.nil?
    raise e
  end

  def unprovision(name, bindings)
    @logger.info("started to unprovision vblob service: #{name}")
    provisioned_service = ProvisionedService.get(name)
    occupied_port = provisioned_service.port
    raise ServiceError.new(ServiceError::NOT_FOUND, name) if provisioned_service.nil?
    raise "Could not cleanup service #{provisioned_service.errors.inspect}" unless provisioned_service.delete
    free_port(occupied_port)
    @logger.info("Successfully fulfilled unprovision request: #{name}.")
    true
  end

  # provide the key/secret to vblob gw
  def bind(name, bind_opts=nil, credential = nil)
    @logger.debug("Bind request: name=#{name}, bind_opts=#{bind_opts}")
    bind_opts ||= BIND_OPT

    provisioned_service = ProvisionedService.get(name)
    raise ServiceError.new(ServiceError::NOT_FOUND, name) if provisioned_service.nil?

    username = credential && credential['username'] ? credential['username'] : UUIDTools::UUID.random_create.to_s
    password = credential && credential['password'] ? credential['password'] : UUIDTools::UUID.random_create.to_s

    provisioned_service.add_user(username, password, bind_opts)

    host = get_host
    response = {
      "hostname" => host,
      "host"     => host,
      "port"     => provisioned_service.port,
      "username" => username,
      "password" => password,
      "name"     => provisioned_service.name,
    }
    @logger.debug("response: #{response}")
    response
  end

  def unbind(credential)
    @logger.debug("Unbind request: credential=#{credential}")
    provisioned_service = ProvisionedService.get(credential['name'])

    raise ServiceError.new(ServiceError::NOT_FOUND, credential['name']) if provisioned_service.nil?
    raise ServiceError.new(ServiceError::HTTP_BAD_REQUEST) unless provisioned_service.port == credential['port']

    provisioned_service.remove_user(credential['username'], credential['password'])
    @logger.debug("Successfully unbind #{credential}")
    true
  end

  def varz_details
    varz = {}
    varz[:max_capacity] = @max_capacity
    varz[:available_capacity] = @capacity

    # check NFS disk free space
    free_space = 0
    begin
      stats = Filesystem.stat("#{@base_dir}")
      avail_blocks = stats.blocks_available
      total_blocks = stats.blocks
      free_space = format("%.2f", avail_blocks.to_f / total_blocks.to_f * 100)
    rescue => e
      @logger.error("Failed to get filesystem info of #{@base_dir}: #{e}")
    end
    varz[:nfs_free_space] = free_space

    # check instances health status
    varz[:instances] = {}
    ProvisionedService.all.each do |provisioned_service|
      varz[:instances][provisioned_service.name.to_sym] = get_healthz(provisioned_service)
    end

    varz
  end

  def get_healthz(provisioned_service)
    Timeout::timeout(VBLOB_TIMEOUT) { Net::HTTP.start(provisioned_service[:ip], provisioned_service.service_port) { |http| response = http.get("/")} }
    "ok"
  rescue => e
    @logger.warn("Getting healthz for #{provisioned_service.inspect} failed with error: #{e}")
    "fail"
  end

  # down-below for warden
  def disable_instance(service_credential, binding_credentials)
    @logger.info("disable_instance request: service_credential=#{service_credential}, binding_credentials=#{binding_credentials}")
    provisioned_service = ProvisionedService.get(service_credential['name'])
    raise ServiceError.new(ServiceError::NOT_FOUND, service_credential['name']) if provisioned_service.nil?
    provisioned_service.stop if provisioned_service.running?
    true
  rescue => e
    @logger.warn(e)
    nil
  end

  def enable_instance(service_credential, binding_credentials)
    @logger.info("enable_instance request: service_credential=#{service_credential}, binding_credentials=#{binding_credentials}")
    provisioned_service = ProvisionedService.get(service_credential['name'])
    raise ServiceError.new(ServiceError::NOT_FOUND, service_credential['name']) if provisioned_service.nil?
    provisioned_service.run unless provisioned_service.running?
    raise VBlobError.new(VBlobError::VBLOB_START_INSTANCE_ERROR) if wait_service_start(provisioned_service) == false
    true
  rescue => e
    @logger.warn(e)
    nil
  end

  def is_service_started(instance)
    get_healthz(instance) == "ok" ? true : false
  end
end


class VCAP::Services::VBlob::Node::ProvisionedService

  include DataMapper::Resource
  include VCAP::Services::Base::Utils
  include VCAP::Services::Base::Warden
  include VCAP::Services::VBlob

  VBLOB_TIMEOUT = 3

  property :name,         String,   :key => true
  property :port,         Integer,  :unique => true
  property :pid,          Integer
  property :memory,       Integer
  property :keyid,        String,   :required => true
  property :secretid,     String,   :required => true
  property :container,    String
  property :ip,           String

  private_class_method :new

  class << self

    include VCAP::Services::VBlob

    def init(options)
      @base_dir = options[:base_dir]
      @log_dir = options[:vblobd_log_dir]
      @max_db_size = options[:max_db_size] || 2048 #default max megabytes
      @logger = options[:logger]
      @@config_template = ERB.new(File.read(options[:config_template]))
      @@vblobd_path = options[:vblobd_path]
      @@vblobd_auth = options[:vblobd_auth] || "basic" #default is basic auth
      @@vblobd_obj_limit = options[:vblobd_obj_limit] || 32768  #default max obj num
      FileUtils.mkdir_p(base_dir)
      FileUtils.mkdir_p(log_dir)
      DataMapper.setup(:default, options[:local_db])
      DataMapper::auto_upgrade!
    end

    def create(port, name, username, password)
      provisioned_service             = new
      provisioned_service.name        = name
      provisioned_service.port        = port
      provisioned_service.keyid       = username
      provisioned_service.secretid    = password
      raise "Cannot save provision_service" unless provisioned_service.save!

      provisioned_service.prepare_filesystem(max_db_size)
      FileUtils.mkdir_p(provisioned_service.data_dir)

      provisioned_service.generate_config
      provisioned_service
    end
  end

  def generate_config
    provisioned_service = self
    vblob_root_dir = "/store/instance/vblob_data"
    vblob_node_path = "/usr/bin/node"
    log_file = "/store/log/vblob.log"
    account_file = File.join("/store/instance/", "account.json")
    config_file = File.join("/store/instance/", "config.json")
    vblobd_quota = provisioned_service.class.max_db_size * 1024 * 1024
    config = @@config_template.result(binding)

    config_path = File.join(provisioned_service.base_dir, "config.json")
    File.open(config_path, "w") {|f| f.write(config)}
  end

  def service_port
    25001
  end

  def service_script
    "vblob_startup.sh"
  end

  def data_dir
    File.join(base_dir,'vblob_data')
  end

  def data_dir?
    Dir.exists?(data_dir)
  end

  def add_user(username, password, bind_opts)
    logger.debug("add user #{username} in port: #{self[:port]}")
    credentials = "{\"#{username}\":\"#{password}\"}"
    response = Net::HTTP::start(self[:ip], service_port) do |http|
      http.open_timeout = http.read_timeout = VBLOB_TIMEOUT
      http.send_request('PUT', '/~bind', credentials, auth_header(self[:keyid], self[:secretid]))
    end
    raise VBlobError.new(VBlobError::VBLOB_ADD_USER_ERROR, options[:username]) if (response.nil? || response.code != "200")
    logger.debug("user #{username} added")
  end

  def remove_user(username, password)
    logger.debug("remove remove #{username} in port: #{self[:port]}")
    credentials = "{\"#{username}\":\"#{password}\"}"
    response = Net::HTTP::start(self[:ip], service_port) do |http|
      http.open_timeout = http.read_timeout = VBLOB_TIMEOUT
      http.send_request('PUT', '/~unbind', credentials, auth_header(self[:keyid], self[:secretid]))
    end
    raise VBlobError.new(VBlobError::VBLOB_REMOVE_USER_ERROR, username) if (response.nil? || response.code != "200")
    logger.debug("user #{username} removed")
  end

  def auth_header(username, password)
    {"Authorization" => "Basic " + Base64.strict_encode64("#{username}:#{password}").strip}
  end

  def migration_check
    super
    FileUtils.rm_rf(File.join(base_dir, "config.json"))
    generate_config
  end

end
