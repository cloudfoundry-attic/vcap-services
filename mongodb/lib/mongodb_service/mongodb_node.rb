# Copyright (c) 2009-2011 VMware, Inc.
require "erb"
require "fileutils"
require "logger"
require "pp"
require "set"
require "mongo"

require "datamapper"
require "nats/client"
require "uuidtools"

require 'vcap/common'
require 'vcap/component'
require "mongodb_service/common"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..', 'base', 'lib')
require 'base/node'

module VCAP
  module Services
    module MongoDB
      class Node < VCAP::Services::Base::Node
      end
    end
  end
end

class VCAP::Services::MongoDB::Node

  include VCAP::Services::MongoDB::Common

  # FIXME only support rw currently
  BIND_OPT = 'rw'

  class ProvisionedService
    include DataMapper::Resource
    property :name,       String,   :key => true
    property :port,       Integer,  :unique => true
    property :password,   String,   :required => true
    property :plan,       Enum[:free], :required => true
    property :pid,        Integer
    property :memory,     Integer
    property :admin,      String,   :required => true
    property :adminpass,  String,   :required => true
    property :db,         String,   :required => true

    def listening?
      begin
        TCPSocket.open('localhost', port).close
        return true
      rescue => e
        return false
      end
    end

    def running?
      VCAP.process_running? pid
    end

    def kill(sig=9)
      Process.kill(sig, pid) if running?
    end
  end

  def initialize(options)
    super(options)
    @base_dir = options[:base_dir]
    FileUtils.mkdir_p(@base_dir)
    @mongod_path = options[:mongod_path]

    @available_memory = options[:available_memory]
    @max_memory = options[:max_memory]

    @config_template = ERB.new(File.read(options[:config_template]))

    DataMapper.setup(:default, options[:local_db])
    DataMapper::auto_upgrade!

    @free_ports = Set.new
    options[:port_range].each {|port| @free_ports << port}

    ProvisionedService.all.each do |provisioned_service|
      @free_ports.delete(provisioned_service.port)
      if provisioned_service.listening?
        @logger.info("Service #{provisioned_service.name} already listening on port #{provisioned_service.port}")
        @available_memory -= (provisioned_service.memory || @max_memory)
        next
      end
      begin
        pid = start_instance(provisioned_service)
        provisioned_service.pid = pid
        unless provisioned_service.save
          provisioned_service.kill
          raise "Couldn't save pid (#{pid})"
        end
      rescue => e
        @logger.warn("Error starting service #{provisioned_service.name}: #{e}")
      end
    end

  end

  def shutdown
    super
    @logger.info("Shutting down instances..")
    ProvisionedService.all.each { |provisioned_service| provisioned_service.kill(:SIGTERM) }
  end

  def announcement
    a = {
      :available_memory => @available_memory
    }
    a
  end


  def provision(plan)
    port = @free_ports.first
    @free_ports.delete(port)

    provisioned_service           = ProvisionedService.new
    provisioned_service.name      = "mongodb-#{UUIDTools::UUID.random_create.to_s}"
    provisioned_service.port      = port
    provisioned_service.plan      = plan
    provisioned_service.password  = UUIDTools::UUID.random_create.to_s
    provisioned_service.memory    = @max_memory
    provisioned_service.pid       = start_instance(provisioned_service)
    provisioned_service.admin     = 'admin'
    provisioned_service.adminpass = UUIDTools::UUID.random_create.to_s
    provisioned_service.db        = 'db'

    unless provisioned_service.save
      cleanup_service(provisioned_service)
      raise "Could not save entry: #{provisioned_service.errors.pretty_inspect}"
    end

    begin
      mongodb_add_admin({
        :port      => provisioned_service.port,
        :username  => provisioned_service.admin,
        :password  => provisioned_service.adminpass,
        :db        => provisioned_service.db,
        :timeout   => 10
      })
      mongodb_add_admin({
        :port      => provisioned_service.port,
        :username  => provisioned_service.admin,
        :password  => provisioned_service.adminpass,
        :db        => 'admin',
        :timeout   => 3
      })
    rescue => e
      cleanup_service(provisioned_service)
      raise e.to_s + ": Could not save admin user."
    end

    response = {
      "hostname" => @local_ip,
      "port" => provisioned_service.port,
      "password" => provisioned_service.password,
      "name" => provisioned_service.name,
      "db" => provisioned_service.db,
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
    @logger.debug("Killing #{provisioned_service.name} started with pid #{provisioned_service.pid}")
    raise "Could not cleanup service: #{provisioned_service.errors.pretty_inspect}" unless provisioned_service.destroy

    Process.kill(9, provisioned_service.pid) if provisioned_service.running?

    dir = File.join(@base_dir, provisioned_service.name)

    EM.defer { FileUtils.rm_rf(dir) }

    @available_memory += provisioned_service.memory
    @free_ports << provisioned_service.port

    true
  rescue => e
    @logger.warn(e)
  end

  def bind(name, bind_opts)
    @logger.debug("Bind request: name=#{name}, bind_opts=#{bind_opts}")
    bind_opts ||= BIND_OPT

    provisioned_service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if provisioned_service.nil?

    username = UUIDTools::UUID.random_create.to_s
    password = UUIDTools::UUID.random_create.to_s

    mongodb_add_user({
      :port      => provisioned_service.port,
      :admin     => provisioned_service.admin,
      :adminpass => provisioned_service.adminpass,
      :db        => provisioned_service.db,
      :username  => username,
      :password  => password,
      :bindopt   => bind_opts
    })

    response = {
      "hostname" => @local_ip,
      "port"    => provisioned_service.port,
      "username" => username,
      "password" => password,
      "name"     => provisioned_service.name,
      "db"       => provisioned_service.db
    }

    @logger.debug("response: #{response}")
    response
  end

  def unbind(credentials)
    @logger.debug("Unbind request: credentials=#{credentials}")

    name = credentials['name']
    provisioned_service = ProvisionedService.get(name)
    raise ServiceError.new(ServiceError::NOT_FOUND, name) if provisioned_service.nil?

    # FIXME  Current implementation: Delete self
    #        Here I presume the user to be deleted is RW user
    mongodb_remove_user({
        :port      => credentials['port'],
        :admin     => provisioned_service.admin,
        :adminpass => provisioned_service.adminpass,
        :username  => credentials['username'],
        :db        => credentials['db']
      })

    @logger.debug("Successfully unbind #{credentials}")
    true
  end

  def start_instance(provisioned_service)
    @logger.debug("Starting: #{provisioned_service.pretty_inspect}")

    memory = @max_memory

    pid = fork
    if pid
      @logger.debug("Service #{provisioned_service.name} started with pid #{pid}")
      @available_memory -= memory
      # In parent, detach the child.
      Process.detach(pid)
      pid
    else
      $0 = "Starting MongoDB service: #{provisioned_service.name}"
      close_fds

      port = provisioned_service.port
      password = provisioned_service.password
      dir = File.join(@base_dir, provisioned_service.name)
      data_dir = File.join(dir, "data")
      log_file = File.join(dir, "log")

      config = @config_template.result(binding)
      config_path = File.join(dir, "mongodb.conf")

      FileUtils.mkdir_p(dir)
      FileUtils.mkdir_p(data_dir)
      FileUtils.rm_f(log_file)
      FileUtils.rm_f(config_path)
      File.open(config_path, "w") {|f| f.write(config)}

      exec("#{@mongod_path} -f #{config_path}")
    end
  end

  def memory_for_service(provisioned_service)
    case provisioned_service.plan
      when :free then 256
      else
        raise "Invalid plan: #{provisioned_service.plan}"
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

  def mongodb_add_admin(options)
    @logger.info("add admin user: req #{options}")

    timeout = EM.add_timer(options[:timeout]) do
      EM.cancel_timer(timer)
      raise "Could not add admin in #{options[:port]}"
    end

    timer = EM.add_periodic_timer(0.50) do
      begin
        db = Mongo::Connection.new('127.0.0.1', options[:port]).db(options[:db])
        user = db.add_user(options[:username], options[:password])
        unless user.nil?
          @logger.debug("user added")
          EM.cancel_timer(timer)
          EM.cancel_timer(timeout)
        end
      rescue => e
        @logger.warn("add user #{options[:username]} failed! #{e}")
        raise e
      end
    end
  end

  def mongodb_add_user(options)
    @logger.debug("add user in port: #{options[:port]}, db: #{options[:db]}")
    db = Mongo::Connection.new('127.0.0.1', options[:port]).db(options[:db])
    auth = db.authenticate(options[:admin], options[:adminpass])
    db.add_user(options[:username], options[:password])
    @logger.debug("user #{options[:username]} added")
  end

  def mongodb_remove_user(options)
    @logger.debug("remove user in port: #{options[:port]}, db: #{options[:db]}")
    db = Mongo::Connection.new('127.0.0.1', options[:port]).db(options[:db])
    auth = db.authenticate(options[:admin], options[:adminpass])
    db.remove_user(options[:username])
    @logger.debug("user #{options[:username]} removed")
  end

end
