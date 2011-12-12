# Copyright (c) 2009-2011 VMware, Inc.
# puts "LOAD_PATH: #{$LOAD_PATH}"
require "erb"
require "fileutils"
require "logger"
require "pp"
require "set"
require "timeout"

require "nats/client"
require "uuidtools"

require 'rest_client'
require 'vcap/common'
require 'vcap/component'
require "mvstore_service/common"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..', 'base', 'lib')
require 'base/node'
require "datamapper_l"

module VCAP
  module Services
    module MVStore
      class Node < VCAP::Services::Base::Node
      end
    end
  end
end

class VCAP::Services::MVStore::Node

  include VCAP::Services::MVStore::Common

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

    def kill(sig=:SIGTERM)
      @wait_thread = Process.detach(pid)
      Process.kill(sig, pid) if running?
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
    @mvstore_path = options[:mvstore_path]
    @mvstore_log_dir = options[:mvstore_log_dir]

    @total_memory = options[:available_memory]
    @available_memory = options[:available_memory]
    @max_memory = options[:max_memory]

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

  def inc_memory(memory)
    @mutex.synchronize do
      @available_memory += memory
    end
  end

  def dec_memory(memory)
    @mutex.synchronize do
      @available_memory -= memory
    end
  end

  def pre_send_announcement
    ProvisionedService.all.each do |provisioned_service|
      delete_port(provisioned_service.port)
      if provisioned_service.listening?
        @logger.warn("Service #{provisioned_service.name} already listening on port #{provisioned_service.port}")
        dec_memory(provisioned_service.memory || @max_memory)
        next
      end

      unless service_exist?(provisioned_service)
        @logger.warn("Service #{provisioned_service.name} in local DB, but not in file system")
        next
      end

      begin
        pid = start_instance(provisioned_service)
        provisioned_service.pid = pid
        @logger.info("started mvstore service pid #{pid}")
        raise "Cannot save provisioned_service" unless provisioned_service.save
      rescue => e
        @logger.error("Error starting service #{provisioned_service.name}: #{e}")
        provisioned_service.kill
      end
    end
  end

  def shutdown
    super
    @logger.info("Shutting down instances..")
    ProvisionedService.all.each { |provisioned_service|
      @logger.debug("Try to terminate mvstore pid:#{provisioned_service.pid}")
      provisioned_service.kill(:SIGTERM)
      provisioned_service.wait_killed ?
        @logger.debug("mvstore pid:#{provisioned_service.pid} terminated") :
        @logger.error("Timeout to terminate mvstore pid:#{provisioned_service.pid}")
    }
  end

  def announcement
    a = {
      :available_memory => @available_memory
    }
    a
  end

  def all_instances_list
    ProvisionedService.all.map{|ps| ps["name"]}
  end

  def all_bindings_list
    list = []
    ProvisionedService.all.each do |instance|
      begin
        adminname = "#{instance.admin}".gsub(/\-/, "")
        adminpw = "#{instance.adminpass}".gsub(/\-/, "")
        adminurlbase = "http://#{adminname}:#{adminpw}@localhost:#{instance.port}/db"
        adminurl = "#{adminurlbase}?q=#{CGI::escape("SELECT * WHERE EXISTS(username);")}&i=mvsql&o=json"
        pins = JSON.parse(RestClient.get adminurl)
        pins.each do |p|
          credential = {
            'name' => instance.name,
            'port' => instance.port,
            'username' => p['username']
          }
          list << credential if credential['username'] != instance.admin
        end
      rescue => e
        @logger.warn("Failed fetch user list: #{e.message}")
      end
    end
    list
  end

  def provision(plan, credential = nil)
    @logger.info("Provision request: plan=#{plan}")
    port = credential && credential['port'] ? fetch_port(credential['port']) : fetch_port
    name = credential && credential['name'] ? credential['name'] : UUIDTools::UUID.random_create.to_s

    # Cleanup instance dir if it exists
    FileUtils.rm_rf(service_dir(name))

    provisioned_service           = ProvisionedService.new
    provisioned_service.name      = name
    provisioned_service.port      = port
    provisioned_service.plan      = plan
    provisioned_service.password  = UUIDTools::UUID.random_create.to_s
    provisioned_service.memory    = @max_memory
    provisioned_service.pid       = start_instance(provisioned_service)
    provisioned_service.admin     = 'admin'
    provisioned_service.adminpass = UUIDTools::UUID.random_create.to_s

    raise "Cannot save provisioned_service" unless provisioned_service.save

    username = credential && credential['username'] ? credential['username'] : UUIDTools::UUID.random_create.to_s
    password = credential && credential['password'] ? credential['password'] : UUIDTools::UUID.random_create.to_s

    # wait for mvstore to start
    sleep 0.5

    # Note (maxw):
    #   basic multi-tenancy is essentially managed by our store server;
    #   for each new {username,password} pair, the server will create a new
    #   encrypted store. For the moment, I also maintain a separate store for
    #   a very simple 'admin' account (purely to implement things like
    #   all_bindings_list).
    adminname = "#{provisioned_service.admin}".gsub(/\-/, "")
    adminpw = "#{provisioned_service.adminpass}".gsub(/\-/, "")
    adminurlbase = "http://#{adminname}:#{adminpw}@localhost:#{provisioned_service.port}/db"
    adminurl = "#{adminurlbase}?q=#{CGI::escape("INSERT username='#{username}', password='#{password}';")}&i=mvsql&o=json"
    response = RestClient.get adminurl
    raise "Cannot record provisioned service in admin db" unless JSON.parse(response).size() == 1

    response = {
      "host" => @local_ip,
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

    dir = service_dir(provisioned_service.name)
    log_dir = log_dir(provisioned_service.name)

    EM.defer do
      FileUtils.rm_rf(dir)
      FileUtils.rm_rf(log_dir)
    end

    inc_memory(provisioned_service.memory)
    return_port(provisioned_service.port)

    raise "Could not cleanup service: #{provisioned_service.errors.inspect}" unless provisioned_service.destroy
    true
  end

  def bind(name, bind_opts, credential = nil)
    @logger.info("Bind request: name=#{name}, bind_opts=#{bind_opts}")
    bind_opts ||= BIND_OPT

    provisioned_service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if provisioned_service.nil?

    username = credential && credential['username'] ? credential['username'] : UUIDTools::UUID.random_create.to_s
    password = credential && credential['password'] ? credential['password'] : UUIDTools::UUID.random_create.to_s

    # Note (maxw):
    #   basic multi-tenancy is essentially managed by our store server;
    #   for each new {username,password} pair, the server will create a new
    #   encrypted store. For the moment, I also maintain a separate store for
    #   a very simple 'admin' account (purely to implement things like
    #   all_bindings_list).
    adminname = "#{provisioned_service.admin}".gsub(/\-/, "")
    adminpw = "#{provisioned_service.adminpass}".gsub(/\-/, "")
    adminurlbase = "http://#{adminname}:#{adminpw}@localhost:#{provisioned_service.port}/db"
    adminurl = "#{adminurlbase}?q=#{CGI::escape("INSERT username='#{username}', password='#{password}';")}&i=mvsql&o=json"
    response = RestClient.get adminurl
    raise "Cannot record provisioned service in admin db" unless JSON.parse(response).size() == 1

    response = {
      "host" => @local_ip,
      "port"     => provisioned_service.port,
      "username" => username,
      "password" => password,
      "name"     => provisioned_service.name,
    }

    @logger.debug("Bind response: #{response}")
    response
  end

  def unbind(credential)
    @logger.info("Unbind request: credential=#{credential}")

    name = credential['name']
    provisioned_service = ProvisionedService.get(name)
    raise ServiceError.new(ServiceError::NOT_FOUND, name) if provisioned_service.nil?

    # Review (maxw): Decide what we want to do here...

    @logger.debug("Successfully unbind #{credential}")
    true
  end

  def restore(instance_id, backup_file)
    @logger.info("Restore request: instance_id=#{instance_id}, backup_file=#{backup_file}")
    # Not yet supported.
    nil
  end

  def disable_instance(service_credential, binding_credentials)
    @logger.info("disable_instance request: service_credential=#{service_credential}, binding_credentials=#{binding_credentials}")
    # Not yet supported.
    nil
  end

  def dump_instance(service_credential, binding_credentials, dump_dir)
    @logger.info("dump_instance request: service_credential=#{service_credential}, binding_credentials=#{binding_credentials}, dump_dir=#{dump_dir}")
    # Not yet supported.
    nil
  end

  def import_instance(service_credential, binding_credentials, dump_dir, plan)
    @logger.info("import_instance request: service_credential=#{service_credential}, binding_credentials=#{binding_credentials}, dump_dir=#{dump_dir}, plan=#{plan}")
    # Not yet supported.
    nil
  end

  def enable_instance(service_credential, binding_credentials)
    @logger.info("enable_instance request: service_credential=#{service_credential}, binding_credentials=#{binding_credentials}")
    # Not yet supported.
    nil
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

    # Get mvstore db.stats and db.serverStatus
    stats = []
    ProvisionedService.all.each do |provisioned_service|
      stat = {}
      overall_stats = mvstore_overall_stats({
        :port      => provisioned_service.port,
        :name      => provisioned_service.name,
        :admin     => provisioned_service.admin,
        :adminpass => provisioned_service.adminpass
      })
      db_stats = mvstore_db_stats({
        :port      => provisioned_service.port,
        :name      => provisioned_service.name,
        :admin     => provisioned_service.admin,
        :adminpass => provisioned_service.adminpass,
      })
      stat['overall'] = overall_stats
      stat['db'] = db_stats
      stat['name'] = provisioned_service.name
      stats << stat
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
    ProvisionedService.all.each do |instance|
      healthz[instance.name.to_sym] = get_healthz(instance)
    end
    healthz
  rescue => e
    @logger.warn("Error get healthz details: #{e}")
    {:self => "fail"}
  end

  def get_healthz(instance)
    # Review (maxw): This is done every 30 seconds... we might prefer a lighter request.
    @logger.info("*** checking health at #{@local_ip}")
    url = "http://#{@local_ip}:#{instance.port}/db?q=select+*+from+mv:ClassOfClasses;&i=mvsql&o=json&limit=1"
    response = RestClient.get url
    response.code == 200 ? "ok" : "fail"
  rescue => e
    "fail"
  end

  def start_instance(provisioned_service)
    @logger.info("Starting: #{provisioned_service.inspect}")

    memory = @max_memory

    pid = fork
    if pid
      @logger.debug("Service #{provisioned_service.name} started with pid #{pid}")
      dec_memory(memory)
      # In parent, detach the child.
      Process.detach(pid)
      pid
    else
      $0 = "Starting mvstore service: #{provisioned_service.name}"
      close_fds

      port = provisioned_service.port
      password = provisioned_service.password
      instance_id = provisioned_service.name
      dir = service_dir(instance_id)
      data_dir = data_dir(dir)
      log_dir = log_dir(instance_id)

      FileUtils.mkdir_p(dir)
      FileUtils.mkdir_p(data_dir)
      FileUtils.mkdir_p(log_dir)

      ld_library_path = @mvstore_path.sub(/\/mvstored/, "")
      web_console_dir = @mvstore_path.sub(/\/bin\/mvstored/, "/src/www/")
      cmd = "#{@mvstore_path} -s #{data_dir} -d #{web_console_dir} -p #{port}"
      begin
        pid = exec({"LD_LIBRARY_PATH" => "#{ld_library_path}"}, cmd)
        @logger.info("exec(#{cmd}) succeeded")
      rescue => e
        @logger.warn("exec(#{cmd}) failed: #{e}!")
        pid = nil
      end
      pid
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

  def mvstore_overall_stats(options)
    @logger.debug("requested mvstore_overall_stats")
    # Review (maxw):
    #   Currently our server doesn't provide any such stats, but we can easily
    #   improve this (memory usage, thread usage, number of stores open, number of
    #   clients per store, etc.), similar to what was available in the server at pi).
    { :process => "mvstored" }
  end

  def mvstore_db_stats(options)
    @logger.debug("requested mvstore_db_stats")
    # Review (maxw):
    #   Currently our server doesn't provide any such stats, but we can easily
    #   improve this (tracking of sessions/pins/cursors/classes etc., similar to
    #   what was available in the server at pi).
    nil
  end

  def service_dir(service_id)
    File.join(@base_dir, service_id)
  end

  def dump_file(to_dir)
    File.join(to_dir, 'dump_file')
  end

  def log_file(instance_id)
    File.join(log_dir(instance_id), 'mvstore.log')
  end

  def log_dir(instance_id)
    File.join(@mvstore_log_dir, instance_id)
  end

  def data_dir(base_dir)
    File.join(base_dir, 'data')
  end

  def service_exist?(provisioned_service)
    Dir.exists?(service_dir(provisioned_service.name))
  end

  def record_service_log(service_id)
    @logger.warn(" *** BEGIN mvstore log - instance: #{service_id}")
    @logger.warn("")
    file = File.new(log_file(service_id), 'r')
    while (line = file.gets)
      @logger.warn(line.chomp!)
    end
  rescue => e
    @logger.warn(e)
  ensure
    @logger.warn(" *** END mvstore log - instance: #{service_id}")
    @logger.warn("")
  end
end
