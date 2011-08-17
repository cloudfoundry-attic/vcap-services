# Copyright (c) 2009-2011 VMware, Inc.
require "erb"
require "fileutils"
require "logger"
require "pp"
require "set"
require "mongo"
require "timeout"

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

    @total_memory = options[:available_memory]
    @available_memory = options[:available_memory]
    @max_memory = options[:max_memory]

    @config_template = ERB.new(File.read(options[:config_template]))

    DataMapper.setup(:default, options[:local_db])
    DataMapper::auto_upgrade!

    @free_ports = Set.new
    options[:port_range].each {|port| @free_ports << port}
  end

  def pre_send_announcement
    ProvisionedService.all.each do |provisioned_service|
      @free_ports.delete(provisioned_service.port)
      if provisioned_service.listening?
        @logger.warn("Service #{provisioned_service.name} already listening on port #{provisioned_service.port}")
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
    ProvisionedService.all.each { |provisioned_service|
      @logger.debug("Try to terminate mongod pid:#{provisioned_service.pid}")
      provisioned_service.kill(:SIGTERM)
      provisioned_service.wait_killed ?
        @logger.debug("mongod pid:#{provisioned_service.pid} terminated") :
        @logger.warn("Timeout to terminate mongod pid:#{provisioned_service.pid}")
    }
  end

  def announcement
    a = {
      :available_memory => @available_memory
    }
    a
  end


  def provision(plan, credential = nil)
    port = credential && credential['port'] && @free_ports.include?(credential['port']) ? credential['port'] : @free_ports.first
    name = credential && credential['name'] ? credential['name'] : UUIDTools::UUID.random_create.to_s
    db   = credential && credential['db']   ? credential['db']   : 'db'

    @free_ports.delete(port)

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
    provisioned_service.db        = db

    unless provisioned_service.save
      cleanup_service(provisioned_service)
      raise "Could not save entry: #{provisioned_service.errors.inspect}"
    end

    begin
      username = credential && credential['username'] ? credential['username'] : UUIDTools::UUID.random_create.to_s
      password = credential && credential['password'] ? credential['password'] : UUIDTools::UUID.random_create.to_s

      # wait for mongod to start
      sleep 0.5

      mongodb_add_admin({
        :port      => provisioned_service.port,
        :username  => provisioned_service.admin,
        :password  => provisioned_service.adminpass,
        :times     => 10
      })

      mongodb_add_user({
        :port      => provisioned_service.port,
        :admin     => provisioned_service.admin,
        :adminpass => provisioned_service.adminpass,
        :db        => provisioned_service.db,
        :username  => username,
        :password  => password
      })

    rescue => e
      record_service_log(provisioned_service.name)
      cleanup_service(provisioned_service)
      raise e.to_s + ": Could not save admin user."
    end

    response = {
      "hostname" => @local_ip,
      "host" => @local_ip,
      "port" => provisioned_service.port,
      "name" => provisioned_service.name,
      "db" => provisioned_service.db,
      "username" => username,
      "password" => password
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
    raise "Could not cleanup service: #{provisioned_service.errors.inspect}" unless provisioned_service.destroy

    provisioned_service.kill(:SIGKILL) if provisioned_service.running?

    dir = service_dir(provisioned_service.name)
    log_dir = log_dir(provisioned_service.name)

    EM.defer do
      FileUtils.rm_rf(dir)
      FileUtils.rm_rf(log_dir)
    end

    @available_memory += provisioned_service.memory
    @free_ports << provisioned_service.port

    true
  end

  def bind(name, bind_opts, credential = nil)
    @logger.debug("Bind request: name=#{name}, bind_opts=#{bind_opts}")
    bind_opts ||= BIND_OPT

    provisioned_service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if provisioned_service.nil?

    username = credential && credential['username'] ? credential['username'] : UUIDTools::UUID.random_create.to_s
    password = credential && credential['password'] ? credential['password'] : UUIDTools::UUID.random_create.to_s

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
      "host" => @local_ip,
      "port"     => provisioned_service.port,
      "username" => username,
      "password" => password,
      "name"     => provisioned_service.name,
      "db"       => provisioned_service.db
    }

    @logger.debug("response: #{response}")
    response
  end

  def unbind(credential)
    @logger.debug("Unbind request: credential=#{credential}")

    name = credential['name']
    provisioned_service = ProvisionedService.get(name)
    raise ServiceError.new(ServiceError::NOT_FOUND, name) if provisioned_service.nil?

    # FIXME  Current implementation: Delete self
    #        Here I presume the user to be deleted is RW user
    mongodb_remove_user({
        :port      => credential['port'],
        :admin     => provisioned_service.admin,
        :adminpass => provisioned_service.adminpass,
        :username  => credential['username'],
        :db        => credential['db']
      })

    @logger.debug("Successfully unbind #{credential}")
    true
  end

  def restore(instance_id, backup_file)
    @logger.debug("Restore request: instance_id=#{instance_id}, backup_file=#{backup_file}")

    provisioned_service = ProvisionedService.get(instance_id)
    raise ServiceError.new(ServiceError::NOT_FOUND, instance_id) if provisioned_service.nil?

    username = provisioned_service.admin
    password = provisioned_service.adminpass
    port     = provisioned_service.port
    database = provisioned_service.db

    # Drop original collections
    conn = Mongo::Connection.new('127.0.0.1', port)
    conn.db('admin').authenticate(username, password)
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
  ensure
    conn.close if conn
  end

  def disable_instance(service_credential, binding_credentials)
    @logger.debug("disable_instance service_credential: #{service_credential}, binding_credentials: #{binding_credentials}")
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
    @logger.debug("dump_instance :service_credential #{service_credential}, binding_credentials: #{binding_credentials}, dump_dir: #{dump_dir}")

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
    @logger.debug("import_instance service_credential: #{service_credential}, binding_credentials: #{binding_credentials}, dump_dir: #{dump_dir}, plan: #{plan}")

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
    @logger.debug("enable_instance service_credential: #{service_credential}, binding_credentials: #{binding_credentials}")

    # Load provisioned_service from dumped file
    stored_service = nil
    dest_dir = service_dir(service_credential['name'])
    d_file = dump_file(dest_dir)
    File.open(d_file, 'r') do |f|
      stored_service = Marshal.load(f)
    end
    raise "Cannot parse dumpfile stored_service in #{d_file}" if stored_service.nil?

    # Provision the new instance using dumped instance files
    port = @free_ports.first
    @free_ports.delete(port)

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

    unless provisioned_service.save
      provisioned_service.kill
      raise "Could not save entry: #{provisioned_service.errors.inspect}"
    end

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
      overall_stats = mongodb_overall_stats({
        :port      => provisioned_service.port,
        :name      => provisioned_service.name,
        :admin     => provisioned_service.admin,
        :adminpass => provisioned_service.adminpass
      })
      db_stats = mongodb_db_stats({
        :port      => provisioned_service.port,
        :name      => provisioned_service.name,
        :admin     => provisioned_service.admin,
        :adminpass => provisioned_service.adminpass,
        :db        => provisioned_service.db
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
    conn = Mongo::Connection.new(@local_ip, instance.port)
    auth = conn.db('admin').authenticate(instance.admin, instance.adminpass)
    auth ? "ok" : "fail"
  rescue => e
    "fail"
  ensure
    conn.close if conn
  end

  def start_instance(provisioned_service)
    @logger.debug("Starting: #{provisioned_service.inspect}")

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
      instance_id = provisioned_service.name
      dir = service_dir(instance_id)
      data_dir = data_dir(dir)
      log_file = log_file(instance_id)
      log_dir = log_dir(instance_id)

      config = @config_template.result(binding)
      config_path = File.join(dir, "mongodb.conf")

      FileUtils.mkdir_p(dir)
      FileUtils.mkdir_p(data_dir)
      FileUtils.mkdir_p(log_dir)
      FileUtils.rm_f(config_path)
      File.open(config_path, "w") {|f| f.write(config)}

      cmd = "#{@mongod_path} -f #{config_path}"
      exec(cmd) rescue @logger.warn("exec(#{cmd}) failed!")
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
    t = options[:times] || 1
    conn = nil

    t.times do
      begin
        conn = Mongo::Connection.new('127.0.0.1', options[:port])
        user = conn.db('admin').add_user(options[:username], options[:password])
        raise "user not added" if user.nil?
        @logger.debug("user #{options[:username]} added in db #{options[:db]}")
        return true
      rescue => e
        @logger.warn("Failed add user #{options[:username]}: #{e.message}")
        sleep 1
      end
    end

    raise "Could not add admin user #{options[:username]}"
  ensure
    conn.close if conn
  end

  def mongodb_add_user(options)
    @logger.debug("add user in port: #{options[:port]}, db: #{options[:db]}")
    conn = Mongo::Connection.new('127.0.0.1', options[:port])
    auth = conn.db('admin').authenticate(options[:admin], options[:adminpass])
    db = conn.db(options[:db])
    db.add_user(options[:username], options[:password])
    @logger.debug("user #{options[:username]} added")
  ensure
    conn.close if conn
  end

  def mongodb_remove_user(options)
    @logger.debug("remove user in port: #{options[:port]}, db: #{options[:db]}")
    conn = Mongo::Connection.new('127.0.0.1', options[:port])
    auth = conn.db('admin').authenticate(options[:admin], options[:adminpass])
    db = conn.db(options[:db])
    db.remove_user(options[:username])
    @logger.debug("user #{options[:username]} removed")
  ensure
    conn.close if conn
  end

  def mongodb_overall_stats(options)
    conn = Mongo::Connection.new('127.0.0.1', options[:port])
    auth = conn.db('admin').authenticate(options[:admin], options[:adminpass])
    # The following command is not documented in mongo's official doc.
    # But it works like calling db.serverStatus from client. And 10gen support has
    # confirmed it's safe to call it in such way.
    conn.db('admin').command({:serverStatus => 1})
  rescue => e
    @logger.warn("Failed mongodb_overall_stats: #{e.message}, options: #{options}")
    "Failed mongodb_overall_stats: #{e.message}, options: #{options}"
  ensure
    conn.close if conn
  end

  def mongodb_db_stats(options)
    conn = Mongo::Connection.new('127.0.0.1', options[:port])
    auth = conn.db('admin').authenticate(options[:admin], options[:adminpass])
    conn.db(options[:db]).stats()
  rescue => e
    @logger.warn("Failed mongodb_db_stats: #{e.message}, options: #{options}")
    "Failed mongodb_db_stats: #{e.message}, options: #{options}"
  ensure
    conn.close if conn
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
