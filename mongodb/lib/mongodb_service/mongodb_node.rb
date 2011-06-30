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
    @mongorestore_path = options[:mongorestore_path]

    @total_memory = options[:available_memory]
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


  def provision(plan, credential = nil)
    port = credential && credential['port'] && @free_ports.include?(credential['port']) ? credential['port'] : @free_ports.first
    name = credential && credential['name'] ? credential['name'] : UUIDTools::UUID.random_create.to_s
    db   = credential && credential['db']   ? credential['db']   : 'db'

    @free_ports.delete(port)

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
      raise "Could not save entry: #{provisioned_service.errors.pretty_inspect}"
    end

    begin
      username = credential && credential['username'] ? credential['username'] : UUIDTools::UUID.random_create.to_s
      password = credential && credential['password'] ? credential['password'] : UUIDTools::UUID.random_create.to_s

      # wait for mongod to start
      sleep 0.5

      mongodb_add_admin({
        :port      => provisioned_service.port,
        :username  => [provisioned_service.admin, username],
        :password  => [provisioned_service.adminpass, password],
        :db        => provisioned_service.db,
        :times     => 10
      })
      mongodb_add_admin({
        :port      => provisioned_service.port,
        :username  => [provisioned_service.admin],
        :password  => [provisioned_service.adminpass],
        :db        => 'admin',
        :times     => 3
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
    raise "Could not cleanup service: #{provisioned_service.errors.pretty_inspect}" unless provisioned_service.destroy

    provisioned_service.kill if provisioned_service.running?

    dir = File.join(@base_dir, provisioned_service.name)

    EM.defer { FileUtils.rm_rf(dir) }

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
    db = Mongo::Connection.new('127.0.0.1', port).db(database)
    db.authenticate(username, password)
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
    @logger.debug("disable_instance service_credential: #{service_credential}, binding_credentials: #{binding_credentials}")
    service_id = service_credential['name']
    provisioned_service = ProvisionedService.get(service_id)
    raise ServiceError.new(ServiceError::NOT_FOUND, service_credential['name']) if provisioned_service.nil?
    Process.kill(9, provisioned_service.pid) if provisioned_service.running?
    rm_lockfile(service_id)
    true
  rescue => e
    @logger.warn(e)
    nil
  end

  def dump_instance(service_credential, binding_credentials, dump_dir)
    @logger.debug("dump_instance :service_credential #{service_credential}, binding_credentials: #{binding_credentials}, dump_dir: #{dump_dir}")

    from_dir = service_dir(service_credential['name'])
    FileUtils.mkdir_p(dump_dir)

    provisioned_service = ProvisionedService.get(service_credential['name'])
    raise "Cannot file service #{service_credential['name']}" if provisioned_service.nil?

    d_file = dump_file(dump_dir)
    File.open(d_file, 'w') do |f|
      Marshal.dump(provisioned_service, f)
    end
    FileUtils.cp_r(File.join(from_dir, '.'), dump_dir)
    true
  rescue => e
    @logger.warn(e)
    nil
  end

  def import_instance(service_credential, binding_credentials, dump_dir, plan)
    @logger.debug("import_instance service_credential: #{service_credential}, binding_credentials: #{binding_credentials}, dump_dir: #{dump_dir}, plan: #{plan}")

    to_dir = service_dir(service_credential['name'])
    FileUtils.rm_rf(to_dir)
    FileUtils.mkdir_p(to_dir)
    FileUtils.cp_r(File.join(dump_dir, '.'), to_dir)
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
      raise "Could not save entry: #{provisioned_service.errors.pretty_inspect}"
    end

    # Update credentials for the new credential
    service_credential['port'] = port
    service_credential['host'] = @local_ip
    service_credential['hostname'] = @local_ip

    binding_credentials.each_value do |v|
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
      overall_stats = mongodb_overall_stats({
        :port      => provisioned_service.port,
        :admin     => provisioned_service.admin,
        :adminpass => provisioned_service.adminpass
      }) || {}
      db_stats = mongodb_db_stats({
        :port      => provisioned_service.port,
        :admin     => provisioned_service.admin,
        :adminpass => provisioned_service.adminpass,
        :db        => provisioned_service.db
      })
      overall_stats['db'] = db_stats
      overall_stats['name'] = provisioned_service.name
      stats << overall_stats
    end
    {
      :running_services     => stats,
      :disk                 => du_hash,
      :services_max_memory  => @total_memory,
      :services_used_memory => @total_memory - @available_memory
    }
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
      dir = service_dir(provisioned_service.name)
      data_dir = data_dir(dir)
      log_file = log_file(dir)

      config = @config_template.result(binding)
      config_path = File.join(dir, "mongodb.conf")

      FileUtils.mkdir_p(dir)
      FileUtils.mkdir_p(data_dir)
      FileUtils.rm_f(log_file)
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

    t.times do
      begin
        db = Mongo::Connection.new('127.0.0.1', options[:port]).db(options[:db])
        options[:username].each_index do |i|
          user = db.add_user(options[:username][i], options[:password][i])
          raise "user not added" if user.nil?
          @logger.debug("user #{options[:username][i]} added in db #{options[:db]}")
        end
        return true
      rescue => e
        @logger.warn("add user #{options[:username]} failed! #{e}")
        sleep 1
      end
    end

    raise "Could not add admin user #{options[:username]}"
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

  def mongodb_overall_stats(options)
    db = Mongo::Connection.new('127.0.0.1', options[:port]).db('admin')
    auth = db.authenticate(options[:admin], options[:adminpass])
    # The following command is not documented in mongo's official doc.
    # But it works like calling db.serverStatus from client. And 10gen support has
    # confirmed it's safe to call it in such way.
    db.command({:serverStatus => 1})
  rescue => e
    @logger.warn(e)
    nil
  end

  def mongodb_db_stats(options)
    db = Mongo::Connection.new('127.0.0.1', options[:port]).db(options[:db])
    auth = db.authenticate(options[:admin], options[:adminpass])
    db.stats()
  rescue => e
    @logger.warn(e)
    nil
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

  def log_file(base_dir)
    File.join(base_dir, 'log')
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
    base_dir = service_dir(service_id)
    file = File.new(log_file(base_dir), 'r')
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
