# Copyright (c) 2009-2011 VMware, Inc.
require "erb"
require "fileutils"
require "logger"
require "pp"
require "set"
require "mongo"
require "timeout"

require "nats/client"
require "uuidtools"

require 'vcap/common'
require 'vcap/component'
require "mongodb_service/common"

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

  # Timeout for mongo client operations, node cannot be blocked on any mongo instances.
  # Default value is 2 seconds
  MONGO_TIMEOUT = 2

  # Max clients' connection number per instance
  MAX_CLIENTS = 500

  # Quota files specify the db quota a instance can use
  QUOTA_FILES = 4

  class ProvisionedService
    include DataMapper::Resource
    property :name,       String,   :key => true
    property :port,       Integer,  :unique => true
    property :password,   String,   :required => true
    # property plan is deprecated. The instances in one node have same plan.
    property :plan,       Integer, :required => true
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
    @mongod_path = options[:mongod_path]
    @mongorestore_path = options[:mongorestore_path]
    @mongod_log_dir = options[:mongod_log_dir]

    @max_memory = options[:max_memory]
    @max_clients = options[:max_clients] || MAX_CLIENTS
    @quota_files = options[:quota_files] || QUOTA_FILES

    @config_template = ERB.new(File.read(options[:config_template]))

    @connection_pool = {}
    @connection_mutex = Mutex.new

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
      @logger.debug("Try to terminate mongod pid:#{provisioned_service.pid}")
      provisioned_service.kill(:SIGTERM)
      provisioned_service.wait_killed ?
        @logger.debug("mongod pid:#{provisioned_service.pid} terminated") :
        @logger.error("Timeout to terminate mongod pid:#{provisioned_service.pid}")
    }
  end

  def announcement
    @capacity_lock.synchronize do
      { :available_capacity => @capacity }
    end
  end

  def all_instances_list
    ProvisionedService.all.map{|ps| ps["name"]}
  end

  def all_bindings_list
    list = []
    ProvisionedService.all.each do |instance|
      begin
        conn = connect_and_auth(instance)
        coll = conn.db(instance.db).collection('system.users')
        coll.find().each do |binding|
          credential = {
            'name' => instance.name,
            'port' => instance.port,
            'db' => instance.db,
            'username' => binding['user']
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
    raise ServiceError.new(MongoDBError::MONGODB_INVALID_PLAN, plan) unless plan == @plan
    port = credential && credential['port'] ? fetch_port(credential['port']) : fetch_port
    name = credential && credential['name'] ? credential['name'] : UUIDTools::UUID.random_create.to_s
    db   = credential && credential['db']   ? credential['db']   : 'db'


    # Cleanup instance dir if it exists
    FileUtils.rm_rf(service_dir(name))

    provisioned_service           = ProvisionedService.new
    provisioned_service.name      = name
    provisioned_service.port      = port
    provisioned_service.plan      = 1
    provisioned_service.password  = UUIDTools::UUID.random_create.to_s
    provisioned_service.memory    = @max_memory
    provisioned_service.pid       = start_instance(provisioned_service)
    provisioned_service.admin     = 'admin'
    provisioned_service.adminpass = UUIDTools::UUID.random_create.to_s
    provisioned_service.db        = db

    raise "Cannot save provision_service" unless provisioned_service.save

    username = credential && credential['username'] ? credential['username'] : UUIDTools::UUID.random_create.to_s
    password = credential && credential['password'] ? credential['password'] : UUIDTools::UUID.random_create.to_s

    # wait for mongod to start
    sleep 0.5

    # Add super_user in admin table for backend operations
    mongodb_add_admin({
      :port      => provisioned_service.port,
      :username  => provisioned_service.admin,
      :password  => provisioned_service.adminpass,
      :times     => 10
    })

    # Add super_user in user table. This user is added to keep node backward
    # compatibile with older version, which depends on this user for backend
    # operations.
    mongodb_add_user(provisioned_service,
                     provisioned_service.admin,
                     provisioned_service.adminpass)

    # Add an end_user
    mongodb_add_user(provisioned_service, username, password)

    response = {
      "hostname" => @local_ip,
      "host" => @local_ip,
      "port" => provisioned_service.port,
      "name" => provisioned_service.name,
      "db" => provisioned_service.db,
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

    close_connection(provisioned_service)

    provisioned_service.kill(:SIGKILL) if provisioned_service.running?

    dir = service_dir(provisioned_service.name)
    log_dir = log_dir(provisioned_service.name)

    EM.defer do
      FileUtils.rm_rf(dir)
      FileUtils.rm_rf(log_dir)
    end

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

    mongodb_add_user(provisioned_service, username, password, bind_opts)

    response = {
      "hostname" => @local_ip,
      "host" => @local_ip,
      "port"     => provisioned_service.port,
      "username" => username,
      "password" => password,
      "name"     => provisioned_service.name,
      "db"       => provisioned_service.db
    }

    @logger.debug("Bind response: #{response}")
    response
  end

  def unbind(credential)
    @logger.info("Unbind request: credential=#{credential}")

    name = credential['name']
    provisioned_service = ProvisionedService.get(name)
    raise ServiceError.new(ServiceError::NOT_FOUND, name) if provisioned_service.nil?

    if provisioned_service.port != credential['port'] or
       provisioned_service.db != credential['db']
      raise ServiceError.new(ServiceError::HTTP_BAD_REQUEST)
    end

    # FIXME  Current implementation: Delete self
    #        Here I presume the user to be deleted is RW user
    mongodb_remove_user(provisioned_service, credential['username'])

    @logger.debug("Successfully unbind #{credential}")
    true
  end

  def restore(instance_id, backup_file)
    @logger.info("Restore request: instance_id=#{instance_id}, backup_file=#{backup_file}")

    provisioned_service = ProvisionedService.get(instance_id)
    raise ServiceError.new(ServiceError::NOT_FOUND, instance_id) if provisioned_service.nil?

    username = provisioned_service.admin
    password = provisioned_service.adminpass
    port     = provisioned_service.port
    database = provisioned_service.db

    # Drop original collections
    conn = connect_and_auth(provisioned_service)
    db = conn.db(database)
    db.collection_names.each do |name|
      if name != 'system.users' && name != 'system.indexes'
        db[name].drop
      end
    end

    # Run mongorestore
    command = "#{@mongorestore_path} -u #{username} -p#{password} --port #{port} #{backup_file}"
    output = `#{command}`
    res = $?.success?
    @logger.debug(output)
    raise 'mongorestore failed' unless res
    true
  end

  def disable_instance(service_credential, binding_credentials)
    @logger.info("disable_instance request: service_credential=#{service_credential}, binding_credentials=#{binding_credentials}")
    service_id = service_credential['name']
    provisioned_service = ProvisionedService.get(service_id)
    raise ServiceError.new(ServiceError::NOT_FOUND, service_credential['name']) if provisioned_service.nil?
    provisioned_service.kill
    rm_lockfile(service_id)
    true
  rescue => e
    @logger.warn(e)
    nil
  end

  def dump_instance(service_credential, binding_credentials, dump_dir)
    @logger.info("dump_instance request: service_credential=#{service_credential}, binding_credentials=#{binding_credentials}, dump_dir=#{dump_dir}")

    instance_id = service_credential['name']
    from_dir = service_dir(instance_id)
    log_dir = log_dir(instance_id)
    FileUtils.mkdir_p(dump_dir)

    provisioned_service = ProvisionedService.get(service_credential['name'])
    raise "Cannot file service #{instance_id}" if provisioned_service.nil?

    d_file = dump_file(dump_dir)
    File.open(d_file, 'w') do |f|
      Marshal.dump(provisioned_service, f)
    end
    FileUtils.cp_r(File.join(from_dir, '.'), dump_dir)
    FileUtils.cp_r(log_dir, dump_dir)
    true
  rescue => e
    @logger.warn(e)
    nil
  end

  def import_instance(service_credential, binding_credentials, dump_dir, plan)
    @logger.info("import_instance request: service_credential=#{service_credential}, binding_credentials=#{binding_credentials}, dump_dir=#{dump_dir}, plan=#{plan}")

    instance_id = service_credential['name']
    to_dir = service_dir(instance_id)
    FileUtils.rm_rf(to_dir)
    FileUtils.mkdir_p(to_dir)
    FileUtils.cp_r(File.join(dump_dir, '.'), to_dir)
    FileUtils.cp_r(File.join(dump_dir, instance_id), @mongod_log_dir)
    true
  rescue => e
    @logger.warn(e)
    nil
  end

  def enable_instance(service_credential, binding_credentials)
    @logger.info("enable_instance request: service_credential=#{service_credential}, binding_credentials=#{binding_credentials}")

    # Load provisioned_service from dumped file
    stored_service = nil
    dest_dir = service_dir(service_credential['name'])
    d_file = dump_file(dest_dir)
    File.open(d_file, 'r') do |f|
      stored_service = Marshal.load(f)
    end
    raise "Cannot parse dumpfile stored_service in #{d_file}" if stored_service.nil?

    # Provision the new instance using dumped instance files
    port = fetch_port

    provisioned_service           = ProvisionedService.new
    provisioned_service.name      = stored_service.name
    provisioned_service.plan      = stored_service.plan
    provisioned_service.password  = stored_service.password
    provisioned_service.memory    = stored_service.memory
    provisioned_service.admin     = stored_service.admin
    provisioned_service.adminpass = stored_service.adminpass
    provisioned_service.db        = stored_service.db
    provisioned_service.port      = port
    provisioned_service.pid       = start_instance(provisioned_service)
    @logger.debug("Provisioned_service: #{provisioned_service}")

    raise "Cannot save provisioned_service" unless provisioned_service.save

    # Update credentials for the new credential
    service_credential['port'] = port
    service_credential['host'] = @local_ip
    service_credential['hostname'] = @local_ip

    binding_credentials.each_value do |value|
      v = value["credentials"]
      v['port'] = port
      v['host'] = @local_ip
      v['hostname'] = @local_ip
    end

    [service_credential, binding_credentials]
  rescue => e
    @logger.warn(e)
    record_service_log(provisioned_service.name)
    cleanup_service(provisioned_service)
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

    # Get mongodb db.stats and db.serverStatus
    stats = []
    ProvisionedService.all.each do |provisioned_service|
      stat = {}
      overall_stats = mongodb_overall_stats(provisioned_service)
      db_stats = mongodb_db_stats(provisioned_service)
      stat['overall'] = overall_stats
      stat['db'] = db_stats
      stat['name'] = provisioned_service.name
      stats << stat
    end
    {
      :running_services     => stats,
      :disk                 => du_hash,
      :max_capacity         => @max_capacity,
      :available_capacity     => @capacity
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

  def connect_and_auth(instance)
    conn = nil
    @connection_mutex.synchronize do
      conn = @connection_pool[instance.port]
      unless conn and conn.connected?
        Timeout::timeout(MONGO_TIMEOUT) do
          conn = Mongo::Connection.new('127.0.0.1', instance.port)
          auth = conn.db('admin').authenticate(instance.admin, instance.adminpass)
          raise "Authentication failed, name: #{instance.name}" unless auth
        end
        @connection_pool[instance.port] = conn
        @logger.debug("Connected to #{instance.name}, port No: #{instance.port}")
      end
    end
    conn
  end

  def close_connection(instance)
    @connection_mutex.synchronize do
      conn = @connection_pool[instance.port]
      conn.close if conn
      @connection_pool[instance.port] = nil
    end
  end

  def get_healthz(instance)
    conn = connect_and_auth(instance)
    "ok"
  rescue => e
    "fail"
  end

  def start_instance(provisioned_service)
    @logger.info("Starting: #{provisioned_service.inspect}")

    memory = @max_memory

    pid = fork
    if pid
      @logger.debug("Service #{provisioned_service.name} started with pid #{pid}")
      # In parent, detach the child.
      Process.detach(pid)
      pid
    else
      $0 = "Starting MongoDB service: #{provisioned_service.name}"
      close_fds

      port = provisioned_service.port
      password = provisioned_service.password
      instance_id = provisioned_service.name
      dir = service_dir(instance_id)
      data_dir = data_dir(dir)
      log_file = log_file(instance_id)
      log_dir = log_dir(instance_id)
      max_clients = @max_clients
      quota_files = @quota_files

      config = @config_template.result(binding)
      config_path = File.join(dir, "mongodb.conf")

      FileUtils.mkdir_p(dir)
      FileUtils.mkdir_p(data_dir)
      FileUtils.mkdir_p(log_dir)
      FileUtils.rm_f(config_path)
      File.open(config_path, "w") {|f| f.write(config)}

      cmd = "#{@mongod_path} -f #{config_path}"
      exec(cmd) rescue @logger.error("exec(#{cmd}) failed!")
    end
  end

  def memory_for_service(provisioned_service)
    256
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
    t = options[:times] || 1
    conn = nil

    t.times do
      begin
        conn = Mongo::Connection.new('127.0.0.1', options[:port])
        user = conn.db('admin').add_user(options[:username], options[:password])
        raise "user not added" if user.nil?
        @logger.debug("user #{options[:username]} added in db admin")
        return true
      rescue => e
        @logger.error("Failed add user #{options[:username]}: #{e.message}")
        sleep 1
      end
    end

    raise "Could not add admin user #{options[:username]}"
  ensure
    conn.close if conn
  end

  def mongodb_add_user(instance, username, password, bind_opts=nil)
    conn = connect_and_auth(instance)
    Timeout::timeout(MONGO_TIMEOUT) do
      conn.db(instance.db).add_user(username, password)
    end
  end

  def mongodb_remove_user(instance, username)
    conn = connect_and_auth(instance)
    Timeout::timeout(MONGO_TIMEOUT) do
      conn.db(instance.db).remove_user(username)
    end
  end

  def mongodb_overall_stats(instance)
    conn = connect_and_auth(instance)

    Timeout::timeout(MONGO_TIMEOUT) do
      # The following command is not documented in mongo's official doc.
      # But it works like calling db.serverStatus from client. And 10gen support has
      # confirmed it's safe to call it in such way.
      conn.db('admin').command({:serverStatus => 1})
    end
  rescue => e
    warning = "Failed mongodb_overall_stats: #{e.message}, instance: #{instance.name}"
    @logger.warn(warning)
    warning
  end

  def mongodb_db_stats(instance)
    conn = connect_and_auth(instance)
    Timeout::timeout(MONGO_TIMEOUT) do
      conn.db(instance.db).stats()
    end
  rescue => e
    warning = "Failed mongodb_db_stats: #{e.message}, instance: #{instance.name}"
    @logger.warn(warning)
    warning
  end

  def transition_dir(service_id)
    File.join(@backup_dir, service_name, service_id)
  end

  def service_dir(service_id)
    File.join(@base_dir, service_id)
  end

  def dump_file(to_dir)
    File.join(to_dir, 'dump_file')
  end

  def log_file(instance_id)
    File.join(log_dir(instance_id), 'mongodb.log')
  end

  def log_dir(instance_id)
    File.join(@mongod_log_dir, instance_id)
  end

  def data_dir(base_dir)
    File.join(base_dir, 'data')
  end

  def service_exist?(provisioned_service)
    Dir.exists?(service_dir(provisioned_service.name))
  end

  def rm_lockfile(service_id)
    lockfile = File.join(service_dir(service_id), 'data', 'mongod.lock')
    FileUtils.rm_rf(lockfile)
  end

  def record_service_log(service_id)
    @logger.warn(" *** BEGIN mongodb log - instance: #{service_id}")
    @logger.warn("")
    file = File.new(log_file(service_id), 'r')
    while (line = file.gets)
      @logger.warn(line.chomp!)
    end
  rescue => e
    @logger.warn(e)
  ensure
    @logger.warn(" *** END mongodb log - instance: #{service_id}")
    @logger.warn("")
  end
end
