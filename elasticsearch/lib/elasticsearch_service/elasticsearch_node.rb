# Copyright (c) 2009-2011 VMware, Inc.
require "erb"
require "fileutils"
require "logger"
require "set"
require "timeout"
require "uuidtools"
require "pp"

require 'vcap/common'
require 'vcap/component'
require "elasticsearch_service/common"
require 'rest-client'
require 'net/http'

module VCAP
  module Services
    module ElasticSearch
      class Node < VCAP::Services::Base::Node
      end
    end
  end
end

class VCAP::Services::ElasticSearch::Node

  include VCAP::Services::ElasticSearch::Common

  # Default value is 2 seconds
  ES_TIMEOUT = 2

  class ProvisionedService
    include DataMapper::Resource
    property :name,       String,       :key => true
    property :port,       Integer,      :unique => true
    property :password,   String,       :required => true
    property :plan,       Enum[:free],  :required => true
    property :pid,        Integer
    property :username,   String,       :required => true

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
    @elasticsearch_log_dir = options[:elasticsearch_log_dir]
    @elasticsearch_path = options[:elasticsearch_path]
    @elasticsearch_plugin_dir = options[:elasticsearch_plugin_dir]
    @logger.debug(options[:elasticsearch_plugin_dir])
    @max_memory = options[:max_memory]
    @config_template = ERB.new(File.read(options[:config_template]))
    @default_logging_config = options[:logging_config_file]

    DataMapper.setup(:default, options[:local_db])
    DataMapper::auto_upgrade!

    @free_ports = Set.new
    options[:port_range].each {|port| @free_ports << port}
    @mutex = Mutex.new
    @supported_versions = ["0.19"]
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

  def announcement
    @capacity_lock.synchronize do
      { :available_capacity => @capacity,
        :capacity_unit => capacity_unit }
    end
  end

  def all_instances_list
    ProvisionedService.all.map{ |ps| ps["name"] }
  end

  def all_bindings_list
    list = []
    ProvisionedService.all.each do |ps|
      begin
        url = "http://#{ps.username}:#{ps.password}@#{@local_ip}:#{ps.port}/_nodes/#{ps.name}"
        response = ''
        Timeout::timeout(ES_TIMEOUT) do
          response = RestClient.get(url)
        end
        credential = {
          'name' => ps.name,
          'port' => ps.port,
          'username' => ps.username
        }
        list << credential if response =~ /"#{ps.name}"/
      rescue => e
        @logger.warn("Failed to fetch status for #{ps.name}: #{e.message}")
      end
    end
    list
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

    # Get elasticsearch health, index & process status
    stats = []
    ProvisionedService.all.each do |provisioned_service|
      stat = {}
      stat['health'] = elasticsearch_health_stats(provisioned_service)
      stat['index'] = elasticsearch_index_stats(provisioned_service)
      stat['process'] = elasticsearch_process_stats(provisioned_service)
      stat['name'] = provisioned_service.name
      stats << stat
    end

    # Get service instance status
    provisioned_instances = {}
    begin
      ProvisionedService.all.each do |instance|
        provisioned_instances[instance.name.to_sym] = elasticsearch_status(instance)
      end
    rescue => e
      @logger.error("Error get instance list: #{e}")
    end

    {
      :running_services     => stats,
      :disk                 => du_hash,
      :max_capacity         => @max_capacity,
      :available_capacity   => @capacity,
      :instances            => provisioned_instances
    }
  end

  def provision(plan, credentials = nil, version=nil)
    provisioned_service = ProvisionedService.new
    if credentials
      provisioned_service.name = credentials["name"]
      provisioned_service.username = credentials["username"]
      provisioned_service.password = credentials["password"]
    else
      provisioned_service.name = "elasticsearch-#{UUIDTools::UUID.random_create.to_s}"
      provisioned_service.username = UUIDTools::UUID.random_create.to_s
      provisioned_service.password = UUIDTools::UUID.random_create.to_s
    end

    provisioned_service.port = fetch_port
    provisioned_service.plan = plan
    provisioned_service.pid = start_instance(provisioned_service)

    unless provisioned_service.pid && provisioned_service.save
      cleanup_service(provisioned_service)
      raise "Could not save entry: #{provisioned_service.errors.pretty_inspect}"
    end

    response = get_credentials(provisioned_service)
    @logger.debug("response: #{response}")
    return response
  rescue => e
    @logger.warn(e)
  end

  def unprovision(name, credentials = nil)
    provisioned_service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if provisioned_service.nil?

    cleanup_service(provisioned_service)
    @logger.debug("Successfully fulfilled unprovision request: #{name}.")
  end

  # FIXME Elasticsearch has no user level security, just return provisioned credentials.
  # Elasticsearch has not built-in user authentication system.
  # So "http-basic(https://github.com/Asquera/elasticsearch-http-basic)" plugin
  # is added for authentication. But It has not support multi-user authentication.
  # It supports only 1 user per 1 instance. Provisioned credentials does not changed
  # regardless of any bind requests.
  def bind(name, bind_opts = 'rw', credentials = nil)
    @logger.debug("Bind request: name=#{name}, bind_opts=#{bind_opts}")

    provisioned_service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if provisioned_service.nil?

    response = get_credentials(provisioned_service)
    @logger.debug("response: #{response}")
    response
  end

  # FIXME Elasticsearch has no user level security, just return.
  def unbind(credentials)
    @logger.debug("Unbind request: credentials=#{credentials}")

    name = credentials['name']
    provisioned_service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if provisioned_service.nil?

    @logger.debug("Successfully unbound #{credentials}")
    true
  end

  def start_instance(provisioned_service)
    @logger.debug("Starting: #{provisioned_service.pretty_inspect}")

    dir = service_dir(provisioned_service.name)

    setup_server(dir, provisioned_service) unless File.directory? "#{dir}/data"

    config_file = config_path(dir)
    pid_file = File.join(dir, "elasticsearch.pid")

    `export ES_HEAP_SIZE="#{@max_memory}m" && #{@elasticsearch_path} -p #{pid_file} -Des.config=#{config_file}`
    status = $?
    @logger.send(status.success? ? :debug : :error, "Start up finished, status = #{status}")

    pid = `[ -f #{pid_file} ] && cat #{pid_file}`
    status = $?
    @logger.send(status.success? ? :debug : :error, "Service #{provisioned_service.name} running with pid #{pid}")

    return pid.to_i
  end

  def elasticsearch_health_stats(instance)
    url = "http://#{instance.username}:#{instance.password}@#{@local_ip}:#{instance.port}/_cluster/health"
    response = nil
    Timeout::timeout(ES_TIMEOUT) do
      response = RestClient.get(url)
    end
    JSON.parse(response) if response
  rescue => e
    warning = "Failed elasticsearch_health_stats: #{e.message}, instance: #{instance.name}"
    @logger.warn(warning)
    warning
  end

  def elasticsearch_index_stats(instance)
    url = "http://#{instance.username}:#{instance.password}@#{@local_ip}:#{instance.port}/_nodes/#{instance.name}/stats"
    response = nil
    Timeout::timeout(ES_TIMEOUT) do
      response = RestClient.get(url)
    end
    JSON.parse(response)['nodes'].flatten[1]['indices']
  rescue => e
    warning = "Failed elasticsearch_index_stats: #{e.message}, instance: #{instance.name}"
    @logger.warn(warning)
    warning
  end

  def elasticsearch_process_stats(instance)
    url = "http://#{instance.username}:#{instance.password}@#{@local_ip}:#{instance.port}/_nodes/#{instance.name}/process"
    response = nil
    Timeout::timeout(ES_TIMEOUT) do
      response = RestClient.get(url)
    end
    JSON.parse(response)['nodes'].flatten[1]['process']
  rescue => e
    warning = "Failed elasticsearch_process_stats: #{e.message}, instance: #{instance.name}"
    @logger.warn(warning)
    warning
  end

  def elasticsearch_status(instance)
    url = "http://#{instance.username}:#{instance.password}@#{@local_ip}:#{instance.port}/_nodes/#{instance.name}"
    Timeout::timeout(ES_TIMEOUT) do
      RestClient.get(url)
    end
    "ok"
  rescue => e
    "fail"
  end

  def setup_server(dir, provisioned_service)
    @logger.info("Installing elasticsearch to #{dir}")

    conf_dir = config_dir(dir)
    data_dir = File.join(dir, 'data')
    work_dir = File.join(dir, 'work')
    logs_dir = log_dir(provisioned_service.name)
    FileUtils.mkdir_p(dir)
    FileUtils.mkdir_p(conf_dir)
    FileUtils.mkdir_p(data_dir)
    FileUtils.mkdir_p(work_dir)
    FileUtils.mkdir_p(logs_dir)

    name = provisioned_service.name
    port = provisioned_service.port
    password = provisioned_service.password
    username = provisioned_service.username
    plugins_dir = @elasticsearch_plugin_dir

    File.open(config_path(dir), "w") { |f| f.write(@config_template.result(binding)) }
    FileUtils.cp(@default_logging_config, conf_dir)
  end

  def get_credentials(provisioned_service)
    raise "Could not access provisioned service" unless provisioned_service
    credentials = {
      "hostname" => @local_ip,
      "host"     => @local_ip,
      "port"     => provisioned_service.port,
      "username" => provisioned_service.username,
      "password" => provisioned_service.password,
      "name"     => provisioned_service.name,
    }
    credentials["url"] = "http://#{credentials['username']}:#{credentials['password']}@#{credentials['host']}:#{credentials['port']}"
    credentials
  end

  def cleanup_service(provisioned_service)
    @logger.debug("Killing #{provisioned_service.name} started with pid #{provisioned_service.pid}")

    stop_service(provisioned_service)
    raise "Could not cleanup service: #{provisioned_service.errors.pretty_inspect}" unless provisioned_service.new? || provisioned_service.destroy
    provisioned_service.kill if provisioned_service.running?

    EM.defer do
      FileUtils.rm_rf(service_dir(provisioned_service.name))
      FileUtils.rm_rf(log_dir(provisioned_service.name))
    end
    return_port(provisioned_service.port)

    true
  rescue => e
    @logger.warn(e)
  end

  def stop_service(service)
    begin
      @logger.info("Stopping #{service.name} PORT #{service.port} PID #{service.pid}")
      service.kill(:SIGTERM) if service.running?
    rescue => e
      @logger.error("Error stopping service #{service.name} PORT #{service.port} PID #{service.pid}: #{e}")
    end
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

  def config_path(dir)
    File.join(config_dir(dir), 'elasticsearch.yml')
  end

  def config_dir(dir)
    File.join(dir, 'config')
  end

  def service_dir(instance_id)
    File.join(@base_dir, instance_id)
  end

  def log_dir(instance_id)
    File.join(@elasticsearch_log_dir, instance_id)
  end
end
