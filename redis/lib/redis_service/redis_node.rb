# Copyright (c) 2009-2011 VMware, Inc.
require "erb"
require "fileutils"
require "logger"
require "pp"

require "datamapper"
require "uuidtools"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..', 'base', 'lib')
require 'base/node'

module VCAP
  module Services
    module Redis
      class Node < VCAP::Services::Base::Node
      end
    end
  end
end

# TODO: these error codes will be defined in base class
REDIS_SAVE_SERVICE_ERROR = 32001
REDIS_DESTORY_SERVICE_ERROR = 32002
REDIS_FIND_SERVICE_ERROR = 32003
REDIS_STOP_SERVICE_ERROR = 32004
REDIS_INVALID_PLAN = 32005

require 'redis_service/common'

class VCAP::Services::Redis::Node

  include VCAP::Services::Redis::Common

  class ProvisionedService
    include DataMapper::Resource
    property :name,       String,   :key => true
    property :port,       Integer,  :unique => true
    property :password,   String,   :required => true
    property :plan,       Enum[:free], :required => true
    property :pid,        Integer
    property :memory,     Integer

    def running?
      VCAP.process_running? pid
    end
  end

  class RedisError < RuntimeError
    attr_reader :code, :description

    def initialize(code, description)
      super(description)
      @code = code
      @description = description
    end  
  end

  def initialize(options)
    super(options)

    @base_dir = options[:base_dir]
    FileUtils.mkdir_p(@base_dir)
    @redis_server_path = options[:redis_server_path]
    @redis_client_path = options[:redis_client_path]
    @available_memory = options[:available_memory]
    @max_memory = options[:max_memory]
    @max_swap = options[:max_swap]
    @config_template = ERB.new(File.read(options[:config_template]))
    @free_ports = Set.new
    options[:port_range].each {|port| @free_ports << port}
    @local_db = options[:local_db]
    @nfs_dir = options[:nfs_dir]
    @options = options
    @disable_password = "disable-#{UUIDTools::UUID.random_create.to_s}"
  end

  def start
    start_db
    start_provisioned_services
  end

  def start_db
    DataMapper.setup(:default, @local_db)
    DataMapper::auto_upgrade!
  end

  def start_provisioned_services
    ProvisionedService.all.each do |service|
      @free_ports.delete(service.port)
      if service.running?
        @logger.info("Service #{service.name} already running with pid #{service.pid}")
        @available_memory -= (service.memory || @max_memory)
        next
      end
      begin
        pid = start_instance(service)
        service.pid = pid
        save_service(service)
      rescue => e
        @logger.warn("Error starting service #{service.name}: #{e}")
      end
    end
  end

  def announcement
    a = {
      :available_memory => @available_memory
    }
  end

  def provision(plan)
    port = @free_ports.first
    @free_ports.delete(port)

    service          = ProvisionedService.new
    service.name     = "redis-#{UUIDTools::UUID.random_create.to_s}"
    service.port     = port
    service.plan     = plan
    service.password = UUIDTools::UUID.random_create.to_s
    service.memory   = @max_memory
    service.pid      = start_instance(service)

    save_service(service)

    credentials = {
      "hostname" => @local_ip,
      "port" => service.port,
      "password" => service.password,
      "name" => service.name
    }
    [true, credentials]
  rescue => e
    @logger.warn(e)
    [false, {:code => e.code, :description => e.description}]
  end

  def unprovision(service_id, handles = {})
    service = get_service(service_id)

    @logger.debug("Killing #{service.name} started with pid #{service.pid}")
    stop_instance(service) if service.running?
    @available_memory += service.memory
    destroy_service(service)
    @free_ports.add(service.port)

    @logger.debug("Successfully fulfilled unprovision request: #{service_id}.")
    [true, {}]
  rescue => e
    @logger.warn(e)
    [false, {:code => e.code, :description => e.description}]
  end

  def bind(service_id, binding_options = :all)
    service = get_service(service_id)
    credentials = {
      "hostname" => @local_ip,
      "port" => service.port,
      "password" => service.password
    }
    [true, credentials]
  rescue => e
    @logger.warn(e)
    [false, {:code => e.code, :description => e.description}]
  end

  def unbind(credentials)
    [true, {}]
  end

  def save_service(service)
    unless service.save
      stop_instance(service)
      raise RedisError.new(REDIS_SAVE_SERVICE_ERROR, "Could not save service: #{service.pretty_inspect}")
    end
  end

  def destroy_service(service)
    raise RedisError.new(REDIS_DESTORY_SERVICE_ERROR, "Could not destroy service: #{service.pretty_inspect}") unless service.destroy
  end

  def get_service(name)
    service = ProvisionedService.get(name)
    raise RedisError.new(REDIS_FIND_SERVICE_ERROR, "Could not find service: #{name}") if service.nil?
    service
  end

  def start_instance(service, db_file = nil)
    @logger.debug("Starting: #{service.pretty_inspect} on port #{service.port}")

    # FIXME: it need call mememory_for_service() to get the memory according to the plan in the further.
    memory = @max_memory

    pid = fork
    if pid
      @logger.debug("Service #{service.name} started with pid #{pid}")
      @available_memory -= memory
      # In parent, detch the child.
      Process.detach(pid)
      pid
    else
      $0 = "Starting Redis service: #{service.name}"
      close_fds

      port = service.port
      password = service.password
      dir = File.join(@base_dir, service.name)
      data_dir = File.join(dir, "data")
      log_file = File.join(dir, "log")
      swap_file = File.join(dir, "redis.swap")
      vm_max_memory = (memory * 0.7).round
      vm_pages = (@max_swap * 1024 * 1024 / 32).round # swap in bytes / size of page (32 bytes)

      config = @config_template.result(Kernel.binding)
      config_path = File.join(dir, "redis.conf")

      FileUtils.mkdir_p(dir)
      FileUtils.mkdir_p(data_dir)
      if db_file
        FileUtils.cp(db_file, data_dir)
      end
      FileUtils.rm_f(log_file)
      FileUtils.rm_f(config_path)
      File.open(config_path, "w") {|f| f.write(config)}

      exec("#{@redis_server_path} #{config_path}")
    end
  end

  def stop_instance(service)
    raise RedisError.new(REDIS_STOP_SERVICE_ERROR, "Could not stop service: #{service.pretty_inspect}") unless %x[#{@redis_client_path} -p #{service.port} -a #{service.password} shutdown] == ""
    dir = File.join(@base_dir, service.name)
    FileUtils.rm_rf(dir)
  end

  def memory_for_service(service)
    case service.plan
      when :free then 16
      else
        raise RedisError.new(REDIS_INVALID_PLAN, "Invalid plan: #{service.plan}")
    end
  end

  def close_fds
    3.upto(get_max_open_fd) do |fd|
      begin
        IO.for_fd(fd, "r").close
      rescue
      end
    end
  end

  def get_max_open_fd
    max = 0

    dir = nil
    if File.directory?("/proc/self/fd/") # Linux
      dir = "/proc/self/fd/"
    elsif File.directory?("/dev/fd/") # Mac
      dir = "/dev/fd/"
    end

    if dir
      Dir.foreach(dir) do |entry|
        begin
          pid = Integer(entry)
          max = pid if pid > max
        rescue
        end
      end
    else
      max = 65535
    end

    max
  end

end
