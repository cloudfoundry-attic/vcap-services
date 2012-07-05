# Copyright (c) 2009-2011 VMware, Inc.
require "erb"
require "fileutils"
require "logger"
require "pp"
require "set"

require "nats/client"
require "uuidtools"

require 'vcap/common'
require 'vcap/component'
require "neo4j_service/common"
require 'rest-client'
require 'net/http'
require 'uri'

module VCAP
  module Services
    module Neo4j
      class Node < VCAP::Services::Base::Node
      end
    end
  end
end

class VCAP::Services::Neo4j::Node

  include VCAP::Services::Neo4j::Common

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
    property :username,      String,   :required => true

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
    @neo4j_path = options[:neo4j_path]
    @max_memory = options[:max_memory]
    @config_template = ERB.new(File.read(options[:config_template]))
    @db_template = ERB.new(File.read(options[:neo4j_template]))
    @log_template = ERB.new(File.read(options[:log4j_template]))

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
          @logger.info("Service #{provisioned_service.name} already listening on port #{provisioned_service.port}")
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
          @logger.error("Error starting service #{provisioned_service.name}: #{e}")
        end
      end
    end
  end

  def shutdown
    super
    @logger.info("Shutting down instances..")
    ProvisionedService.all.each do |service|
      @logger.info("Shutting down #{service}")
      stop_service(service)
    end
  end

  def stop_service(service)
    begin
      @logger.info("Stopping #{service.name} PORT #{service.port} PID #{service.pid}")
      init_script = File.join(@base_dir,service.name,"bin","neo4j")
      @logger.info("Calling #{init_script} stop")
      out = `#{init_script} stop`
      stopped = $?
      @logger.debug("finished stop #{ stopped }i:#{out}")
    rescue => e
      @logger.error("Error stopping service #{service.name} PORT #{service.port} PID #{service.pid}: #{e}")
    end
    service.kill(:SIGTERM) if service.running?
  end

  def announcement
    @capacity_lock.synchronize do
      { :available_capacity => @capacity,
        :capacity_unit => capacity_unit }
    end
  end

  def provision(plan, credentials=nil)
    port = fetch_port

    provisioned_service             = ProvisionedService.new
    if credentials
      provisioned_service.name      = credentials["name"]
      provisioned_service.username  = credentials["username"]
      provisioned_service.password  = credentials["password"]
    else
      provisioned_service.name      = "neo4j-#{UUIDTools::UUID.random_create.to_s}"
      provisioned_service.username  = UUIDTools::UUID.random_create.to_s
      provisioned_service.password  = UUIDTools::UUID.random_create.to_s
    end

    provisioned_service.port        = port
    provisioned_service.plan        = plan
    provisioned_service.memory      = @max_memory
    provisioned_service.pid         = start_instance(provisioned_service)

    unless provisioned_service.pid && provisioned_service.save
      cleanup_service(provisioned_service)
      raise "Could not save entry: #{provisioned_service.errors.pretty_inspect}"
    end

    host = get_host
    response = {
      "hostname" => host,
      "host"     => host,
      "port"     => provisioned_service.port,
      "password" => provisioned_service.password,
      "name"     => provisioned_service.name,
      "username" => provisioned_service.username,
    }
    @logger.debug("response: #{response}")
    return response
  rescue => e
    @logger.warn(e)
  end

  def unprovision(name, credentials)
    provisioned_service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if provisioned_service.nil?

    cleanup_service(provisioned_service)
    @logger.debug("Successfully fulfilled unprovision request: #{name}.")
  end

  def cleanup_service(provisioned_service)
    @logger.debug("Killing #{provisioned_service.name} started with pid #{provisioned_service.pid}")

    stop_service(provisioned_service)

    raise "Could not cleanup service: #{provisioned_service.errors.pretty_inspect}" unless provisioned_service.new? || provisioned_service.destroy

    Process.kill(9, provisioned_service.pid) if provisioned_service.running?
    dir = File.join(@base_dir, provisioned_service.name)

    EM.defer { FileUtils.rm_rf(dir) }

    return_port(provisioned_service.port)

    true
  rescue => e
    @logger.warn(e)
  end

  def bind(name, bind_opts, credentials=nil)
    @logger.debug("Bind request: name=#{name}, bind_opts=#{bind_opts}")
    bind_opts ||= BIND_OPT

    provisioned_service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if provisioned_service.nil?

    if credentials
      username = credentials["username"]
      password = credentials["password"]
    else
      username = UUIDTools::UUID.random_create.to_s
      password = UUIDTools::UUID.random_create.to_s
    end

    ro = bind_opts == "ro"
    r = RestClient.post "http://#{provisioned_service.username}:#{provisioned_service.password}@#{@local_ip}:#{provisioned_service.port}/admin/add-user-#{ro ? 'ro' : 'rw'}","user=#{username}:#{password}"
    raise "Failed to add user:  #{username} status: #{r.code} message: #{r.to_str}" unless r.code == 200

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
  rescue => e
    @logger.warn(e)
    nil
  end

  def unbind(credentials)
    @logger.debug("Unbind request: credentials=#{credentials}")

    name = credentials['name']
    provisioned_service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if provisioned_service.nil?
    username = credentials['username']
    password = credentials['password']
    r = RestClient.post "http://#{provisioned_service.username}:#{provisioned_service.password}@#{@local_ip}:#{provisioned_service.port}/admin/remove-user", "user=#{username}:#{password}"
    raise "Failed to remove user:  #{username} status: #{r.code} message: #{r.to_str}" unless r.code == 200

    @logger.debug("Successfully unbound #{credentials}")
    true
  rescue => e
    @logger.warn(e)
    nil
  end

  def update_config(dir,provisioned_service)
    data_dir = File.join(dir, "data","graph.db")
    port = provisioned_service.port
    password = provisioned_service.password
    username = provisioned_service.username

    @logger.info("Updating Neo4j in #{dir} with port #{port} admin-login #{username}")
    File.open(File.join(dir, "conf","neo4j-server.properties"), "w") {|f| f.write(@config_template.result(binding))}
  end

  def install_server(dir,provisioned_service)
    @logger.info("Installing Neo4j to #{dir} from #{@neo4j_path} name #{provisioned_service.name}")
    `cd #{dir} && tar -xz --strip-components=1 -f #{@neo4j_path}/neo4j-server.tgz`
    `cd #{dir} && rm -rf #{dir}/docs #{dir}/examples`
    `cd #{dir} && cp #{@neo4j_path}/neo4j-hosting-extension.jar #{dir}/system/lib`
    File.open(File.join(dir, "conf","neo4j.properties"), "a") {|f|
      f.write("\nenable_remote_shell=false\nenable_online_backup=false\nenable_statistic_collector=false\n")
    }
  end

  def start_instance(provisioned_service)
    @logger.debug("Starting: #{provisioned_service.pretty_inspect}")

    memory = @max_memory
    name = provisioned_service.name
    dir = File.join(@base_dir, name)
    FileUtils.mkdir_p(dir)

    $0 = "Starting Neo4j service: #{name}"

    data_dir = File.join(dir, "data","graph.db")

    unless File.directory?(data_dir)
      install_server(dir,provisioned_service)
    end
    update_config(dir,provisioned_service)

    init_script=File.join(dir,"bin","neo4j")
    @logger.info("Calling #{init_script} start")

    out = `cd #{dir} && #{init_script} start`
    status = $?
    @logger.send(status.success? ? :debug : :error, "Init finished, status = #{status}: #{out}")

    pidfile = File.join(dir,"data","neo4j-service.pid")

    pid = `[ -f #{pidfile} ] && cat #{pidfile}`
    status = $?
    @logger.send(status.success? ? :debug : :error, "Service #{name} running with pid #{pid} #{status}")

    return pid.to_i
  end

end
