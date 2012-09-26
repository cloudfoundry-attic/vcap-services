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
require "vblob_service/vblob_utils"

require "sys/filesystem"
require "find"
include Sys

module VCAP
  module Services
    module VBlob
      class Node < VCAP::Services::Base::Node
      end
    end
  end
end

class VCAP::Services::VBlob::Node

  VBLOB_TIMEOUT = 3

  include VCAP::Services::VBlob::Common
  include VCAP::Services::VBlob::Utils

  class ProvisionedService
    include DataMapper::Resource
    property :name,         String,   :key => true
    property :port,         Integer,  :unique => true
    property :pid,          Integer
    property :memory,       Integer
    property :keyid,        String,   :required => true
    property :secretid,     String,   :required => true

    def listening?
      begin
        TCPSocket.open('localhost', port).close
        return true
      rescue => e
        return false
      end
    end

    def running?
      return false unless !pid.nil?
      VCAP.process_running? pid
    end

    def kill(sig=:SIGTERM)
      if !pid.nil?
        @wait_thread = Process.detach(pid)
        Process.kill(sig, pid) if running?
      end
    end

    def wait_killed(timeout=5, interval=0.2)
      begin
        Timeout::timeout(timeout) do
          @wait_thread.join if @wait_thread
          while running? do
            sleep interval
          end
        end
      rescue Timeout::Error
        return false
      end
      true
    end
  end

  def initialize(options)
    super(options)
    @base_dir = options[:base_dir]
    FileUtils.mkdir_p(@base_dir)
    @nodejs_path = options[:nodejs_path]
    @vblobd_path = options[:vblobd_path]
    @vblobd_log_dir = options[:vblobd_log_dir]
    @vblobd_auth = options[:vblobd_auth] || "basic" #default is basic auth
    @vblobd_quota = options[:vblobd_quota] || 2147483647 #default max bytes
    @vblobd_obj_limit = options[:vblobd_obj_limit] || 32768  #default max obj num

    @config_template = ERB.new(File.read(options[:config_template]))

    @vblob_start_timeout = 10

    DataMapper.setup(:default, options[:local_db])
    DataMapper::auto_upgrade!

    @free_ports = Set.new
    options[:port_range].each {|port| @free_ports << port}
    @mutex = Mutex.new
  end

  def fetch_port(port=nil)
    @mutex.synchronize do
      port ||= @free_ports.first
      raise "port #{port} is already taken!" unless @free_ports.include?(port)
      @free_ports.delete(port)
      port
    end
  end

  def return_port(port)
    @mutex.synchronize do
      @free_ports << port
    end
  end

  def delete_port(port)
    @mutex.synchronize do
      @free_ports.delete(port)
    end
  end

  def pre_send_announcement
    @capacity_lock.synchronize do
      ProvisionedService.all.each do |provisioned_service|
        @capacity -= capacity_unit
        delete_port(provisioned_service.port)
        if provisioned_service.listening?
          @logger.warn("Service #{provisioned_service.name} already listening on port #{provisioned_service.port}")
          next
        end

        unless service_exist?(provisioned_service)
          @logger.warn("Service #{provisioned_service.name} in local DB, but not in file system")
          next
        end

        begin
          pid = start_instance(provisioned_service)
          provisioned_service.pid = pid
          raise "Cannot save provision_service" unless provisioned_service.save
        rescue => e
          provisioned_service.kill
          @logger.error("Error starting service #{provisioned_service.name}: #{e}")
        end
      end
    end
  end

  def shutdown
    super
    @logger.info("Shutting down instances..")
    ProvisionedService.all.each { |provisioned_service|
      @logger.debug("Trying to terminate vblobd pid:#{provisioned_service.pid}")
      provisioned_service.kill(:SIGTERM)
      provisioned_service.wait_killed ?
        @logger.debug("VBlobd pid:#{provisioned_service.pid} terminated") :
        @logger.error("Timeout to terminate vblobd pid:#{provisioned_service.pid}")
    }
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
    ProvisionedService.all.each do |instance|
      begin
        http = Net::HTTP.new(@local_ip, instance.port)
        request = Net::HTTP::Get.new("/~bind")
        request.basic_auth(instance.keyid, instance.secretid)
        http.open_timeout = http.read_timeout = VBLOB_TIMEOUT
        response = http.request(request)
        raise "Couldn't get binding list" if (!response || response.code != "200")
        bindings = Yajl::Parser.parse(response.body)
        bindings.each_key {|key|
          credential = {
            'name' => instance.name,
            'port' => instance.port,
            'username' => key
          }
          list << credential if credential['username'] != instance.keyid
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

    port = credential && credential['port'] ? fetch_port(credential['port']) : fetch_port
    name   = credential && credential['name'] ? credential['name'] : UUIDTools::UUID.random_create.to_s
    username = credential && credential['username'] ? credential['username'] : UUIDTools::UUID.random_create.to_s
    password = credential && credential['password'] ? credential['password'] : UUIDTools::UUID.random_create.to_s

    # Cleanup instance dir if it exists
    FileUtils.rm_rf(service_dir(name))

    provisioned_service             = ProvisionedService.new
    provisioned_service.name        = name
    provisioned_service.port        = port
    provisioned_service.keyid       = username
    provisioned_service.secretid    = password
    provisioned_service.pid         = start_instance(provisioned_service)

    raise "Cannot save provision_service" unless provisioned_service.save

    # check whether vblob services has been established or not
    1.upto(@vblob_start_timeout) do |t|
      sleep 1
      begin
        Net::HTTP.start(@local_ip, provisioned_service.port) {|http|
          http.open_timeout = http.read_timeout = VBLOB_TIMEOUT
          response = http.get("/")
        }
        break
      rescue => e
        if t == @vblob_start_timeout
          @logger.error("Timeout to start vBlob server for instance #{provisioned_service.name}")
          record_service_log(provisioned_service.name)
          cleanup_service(provisioned_service)
          raise VBlobError.new(VBlobError::VBLOB_START_INSTANCE_ERROR)
        else
          next
        end
      end
    end

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
    record_service_log(provisioned_service.name)
    cleanup_service(provisioned_service)
    raise e
  end

  def unprovision(name, bindings)
    provisioned_service = ProvisionedService.get(name)
    raise ServiceError.new(ServiceError::NOT_FOUND, name) if provisioned_service.nil?
    cleanup_service(provisioned_service)
    @logger.info("Successfully fulfilled unprovision request: #{name}.")
    true
  end

  def cleanup_service(provisioned_service)
    @logger.info("Killing #{provisioned_service.name} started with pid #{provisioned_service.pid}")
    provisioned_service.kill(:SIGKILL) if provisioned_service.running?
    if provisioned_service.wait_killed
      dir = service_dir(provisioned_service.name)
      log_dir = log_dir(provisioned_service.name)
      @logger.debug("vblob pid:#{provisioned_service.pid} terminated")
      EM.defer do
        FileUtils.rm_rf(dir)
        FileUtils.rm_rf(log_dir)
      end
      return_port(provisioned_service.port)
    else
      @logger.error("Timeout to terminate mongod pid:#{provisioned_service.pid}")
    end
    raise VBlobError.new(VBlobError::VBLOB_CLEANUP_ERROR, provisioned_service.errors.pretty_inspect) unless provisioned_service.new? || provisioned_service.destroy
    true
  end

  # provide the key/secret to vblob gw
  def bind(name, bind_opts, credential = nil)
    @logger.debug("Bind request: name=#{name}, bind_opts=#{bind_opts}")
    provisioned_service = ProvisionedService.get(name)
    raise ServiceError.new(ServiceError::NOT_FOUND, name) if provisioned_service.nil?
    username = credential && credential['username'] ? credential['username'] : UUIDTools::UUID.random_create.to_s
    password = credential && credential['password'] ? credential['password'] : UUIDTools::UUID.random_create.to_s

    vblobgw_add_user({
      :port      => provisioned_service.port,
      :admin     => provisioned_service.keyid,
      :adminpass => provisioned_service.secretid,
      :username  => username,
      :password  => password,
      :bindopt   => bind_opts
    })

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
    name = credential['name']
    provisioned_service = ProvisionedService.get(name)
    raise ServiceError.new(ServiceError::NOT_FOUND, name) if provisioned_service.nil?

    if provisioned_service.port != credential['port']
      raise ServiceError.new(ServiceError::HTTP_BAD_REQUEST)
    end

    vblobgw_remove_user({
      :port      => credential['port'],
      :admin     => provisioned_service.keyid,
      :adminpass => provisioned_service.secretid,
      :username  => credential['username'],
      :password  => credential['password']
    })
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

  def get_healthz(instance)
    Net::HTTP.start(@local_ip, instance.port) {|http|
      http.open_timeout = http.read_timeout = VBLOB_TIMEOUT
      response = http.get("/")
    }
    "ok"
  rescue => e
    @logger.warn("Getting healthz for #{instance.inspect} failed with error #{e}")
    "fail"
  end

  def start_instance(provisioned_service)
    @logger.debug("Starting instance: #{provisioned_service.pretty_inspect}")
    pid = Process.fork
    if pid
      @logger.debug("Service #{provisioned_service.name} started with pid #{pid}")
      # In parent, detach the child.
      Process.detach(pid)
      pid
    else
      $0 = "Starting VBlob service: #{provisioned_service.name}"
      close_fds
      vblob_port = provisioned_service.port
      dir = service_dir(provisioned_service.name)
      logdir = log_dir(provisioned_service.name)
      vblob_dir = vblob_dir(dir)
      log_file = log_file_vblob(provisioned_service.name)
      account_file = File.join(dir, "account.json")
      keyid = provisioned_service.keyid
      secretid = provisioned_service.secretid

      config = @config_template.result(binding)
      config_path = File.join(dir, "config.json")
      FileUtils.mkdir_p(dir) rescue @logger.warn("Creating service folder for #{provisioned_service.name} failed")
      FileUtils.mkdir_p(vblob_dir) rescue @logger.warn("Creating vblob data folder for #{provisioned_service.name}  failed")
      FileUtils.mkdir_p(logdir) rescue @logger.warn("Creating log folder for #{provisioned_service.name} failed")
      FileUtils.rm_f(config_path) rescue @logger.warn("Deleting old config file for #{provisioned_service.name} failed")
      File.open(config_path, "w") {|f| f.write(config)}
      cmd = "#{@nodejs_path} #{@vblobd_path}/server.js -f #{config_path}"
      exec(cmd) rescue @logger.warn("exec(#{cmd}) failed!")
    end
  end

  def vblobgw_add_user(options)
    @logger.debug("add user #{options[:username]} in port: #{options[:port]}")
    credentials = "{\"#{options[:username]}\":\"#{options[:password]}\"}";
    response = nil
    #FIXME the inbuilt HTTP put operation seemed to be problematic when running stac;
    #      this has been rolled back to r10, but should fix this problem in r12
    Timeout::timeout(VBLOB_TIMEOUT) do
      response = Net::HTTP.start(@local_ip, options[:port]) {|http|
        http.send_request('PUT','/~bind',credentials, auth_header(options[:admin], options[:adminpass]))
      }
    end
    raise VBlobError.new(VBlobError::VBLOB_ADD_USER_ERROR, options[:username]) if (response.nil? || response.code != "200")
    @logger.debug("user #{options[:username]} added")
  end

  def vblobgw_remove_user(options)
    @logger.debug("remove user #{options[:username]} in port: #{options[:port]}")
    credentials = "{\"#{options[:username]}\":\"#{options[:password]}\"}";
    response = nil
    Timeout::timeout(VBLOB_TIMEOUT) do
      response = Net::HTTP.start(@local_ip, options[:port]) {|http|
        http.send_request('PUT','/~unbind',credentials, auth_header(options[:admin], options[:adminpass]))
      }
    end
    raise VBlobError.new(VBlobError::VBLOB_REMOVE_USER_ERROR, options[:username]) if (response.nil? || response.code != "200")
    @logger.debug("user #{options[:username]} removed")
  end

  def auth_header(user,passwd)
    {"Authorization" => "Basic " + Base64.strict_encode64("#{user}:#{passwd}").strip}
  end

end
