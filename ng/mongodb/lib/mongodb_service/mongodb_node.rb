# Copyright (c) 2009-2011 VMware, Inc.
require "erb"
require "fileutils"
require "logger"

require "nats/client"
require "uuidtools"
require "mongo"
require 'vcap/common'
require 'vcap/component'
require "mongodb_service/common"
require "warden/client"
require "posix/spawn"

module VCAP
  module Services
    module MongoDB
      class Node < VCAP::Services::Base::Node
        class ProvisionedService < VCAP::Services::Base::Warden::Service
        end
      end
    end
  end
end

class VCAP::Services::MongoDB::Node

  include VCAP::Services::MongoDB::Common
  include VCAP::Services::Base::Utils
  include VCAP::Services::Base::Warden::NodeUtils

  # FIXME only support rw currently
  BIND_OPT = 'rw'

  # Timeout for mongo client operations, node cannot be blocked on any mongo instances.
  # Default value is 2 seconds
  MONGO_TIMEOUT = 2

  # Max clients' connection number per instance
  MAX_CLIENTS = 500

  # Quota files specify the db quota a instance can use
  QUOTA_FILES = 4

  def initialize(options)
    super(options)
    ProvisionedService.init(options)
    @base_dir = options[:base_dir]
    init_ports(options[:port_range])
    @service_start_timeout = options[:service_start_timeout] || 3
    @supported_versions  = options[:supported_versions]
    @default_version = options[:default_version]
  end

  def migrate_saved_instances_on_startup
    ProvisionedService.all.each do |p_service|
      if p_service.version.to_s.empty?
        p_service.version = @default_version
        @logger.warn("Unable to set version for: #{p_service.inspect}") unless p_service.save
      end
    end
  end

  def pre_send_announcement
    migrate_saved_instances_on_startup

    @capacity_lock.synchronize do
      start_instances(ProvisionedService.all)
    end
  end

  def shutdown
    super
    @logger.info("Shutting down instances..")
    stop_instances(ProvisionedService.all)
  end

  def announcement
    @capacity_lock.synchronize do
      { :available_capacity => @capacity,
        :capacity_unit => capacity_unit }
    end
  end

  def all_instances_list
    ProvisionedService.all.map { |p_service| p_service["name"] }
  end

  def all_bindings_list
    list = []
    ProvisionedService.all.each do |p_service|
      begin
        conn = p_service.connect
        coll = conn.db(p_service.db).collection('system.users')
        coll.find().each do |binding|
          next if binding['user'] == p_service.admin
          list << {
            'name'     => p_service.name,
            'port'     => p_service.port,
            'db'       => p_service.db,
            'username' => binding['user']
          }
        end
        p_service.disconnect(conn)
      rescue => e
        @logger.warn("Failed fetch user list: #{e.message}")
      end
    end
    list
  end

  def provision(plan, credential = nil, version = nil)
    @logger.info("Provision request: plan=#{plan}, version=#{version}")
    raise ServiceError.new(MongoDBError::MONGODB_INVALID_PLAN, plan) unless plan == @plan
    raise ServiceError.new(ServiceError::UNSUPPORTED_VERSION, version) unless @supported_versions.include?(version)

    credential = {} if credential.nil?
    credential['plan'] = plan
    credential['port'] = new_port(credential['port'])
    credential['version'] = version
    p_service = ProvisionedService.create(credential)
    username = credential['username'] ? credential['username'] : UUIDTools::UUID.random_create.to_s
    password = credential['password'] ? credential['username'] : UUIDTools::UUID.random_create.to_s
    p_service.run(p_service.first_start_options) do
      p_service.add_user(p_service.admin, p_service.adminpass)
      p_service.add_user(username, password)
    end

    host = get_host
    response = {
      "hostname" => host,
      "host"     => host,
      "port"     => p_service.port,
      "name"     => p_service.name,
      "db"       => p_service.db,
      "username" => username,
      "password" => password
    }
    @logger.debug("Provision response: #{response}")
    return response
  rescue => e
    @logger.error("Error provision instance: #{e}")
    p_service.delete unless p_service.nil?
    raise e
  end

  def unprovision(name, bindings)
    p_service = ProvisionedService.get(name)
    raise ServiceError.new(ServiceError::NOT_FOUND, name) if p_service.nil?
    port = p_service.port
    raise "Could not cleanup instance #{name}" unless p_service.delete
    free_port(port);
    @logger.info("Successfully fulfilled unprovision request: #{name}.")
    true
  end

  def bind(name, bind_opts, credential = nil)
    @logger.info("Bind request: name=#{name}, bind_opts=#{bind_opts}")
    bind_opts ||= BIND_OPT

    p_service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if p_service.nil?

    username = credential && credential['username'] ? credential['username'] : UUIDTools::UUID.random_create.to_s
    password = credential && credential['password'] ? credential['password'] : UUIDTools::UUID.random_create.to_s

    p_service.add_user(username, password)

    host = get_host
    response = {
      "hostname" => host,
      "host"     => host,
      "port"     => p_service.port,
      "username" => username,
      "password" => password,
      "name"     => p_service.name,
      "db"       => p_service.db,
      "url"      => "mongodb://#{username}:#{password}@#{host}:#{p_service.port}/#{p_service.db}"
    }

    @logger.debug("Bind response: #{response}")
    response
  end

  def unbind(credential)
    @logger.info("Unbind request: credential=#{credential}")
    p_service = ProvisionedService.get(credential['name'])
    raise ServiceError.new(ServiceError::NOT_FOUND, name) if p_service.nil?

    if p_service.port != credential['port'] or
       p_service.db != credential['db']
      raise ServiceError.new(ServiceError::HTTP_BAD_REQUEST)
    end

    # FIXME  Current implementation: Delete self
    #        Here I presume the user to be deleted is RW user
    p_service.remove_user(credential['username'])

    @logger.debug("Successfully unbind #{credential}")
    true
  end

  def restore(instance_id, backup_file)
    @logger.info("Restore request: instance_id=#{instance_id}, backup_file=#{backup_file}")

    p_service = ProvisionedService.get(instance_id)
    raise ServiceError.new(ServiceError::NOT_FOUND, instance_id) if p_service.nil?

    p_service.d_import(backup_file)
  end

  def disable_instance(service_credential, binding_credentials)
    @logger.info("disable_instance request: service_credential=#{service_credential}, binding_credentials=#{binding_credentials}")
    p_service = ProvisionedService.get(service_credential['name'])
    raise ServiceError.new(ServiceError::NOT_FOUND, service_credential['name']) if p_service.nil?
    p_service.stop if p_service.running?
    true
  rescue => e
    @logger.warn(e)
    nil
  end

  def enable_instance(service_credential, binding_credentials)
    @logger.info("enable_instance request: service_credential=#{service_credential}, binding_credentials=#{binding_credentials}")
    p_service = ProvisionedService.get(service_credential['name'])
    raise ServiceError.new(ServiceError::NOT_FOUND, service_credential['name']) if p_service.nil?
    p_service.run unless p_service.running?
    true
  rescue => e
    @logger.warn(e)
    nil
  end

  def dump_instance(service_credential, binding_credentials, dump_dir)
    @logger.info("dump_instance request: service_credential=#{service_credential}, binding_credentials=#{binding_credentials}, dump_dir=#{dump_dir}")

    p_service = ProvisionedService.get(service_credential['name'])
    raise "Cannot find service #{service_credential['name']}" if p_service.nil?
    FileUtils.mkdir_p(dump_dir)
    p_service.dump(dump_dir)
    true
  rescue => e
    @logger.warn(e)
    nil
  end

  def import_instance(service_credential, binding_credentials, dump_dir, plan)
    @logger.info("import_instance request: service_credential=#{service_credential}, binding_credentials=#{binding_credentials}, dump_dir=#{dump_dir}, plan=#{plan}")

    # Load Provisioned Service from dumped file
    port = new_port
    p_service = ProvisionedService.import(port, dump_dir)
    true
  rescue => e
    @logger.warn(e)
    nil
  end

  def update_instance(service_credential, binding_credentials)
    @logger.info("update_instance request: service_credential=#{service_credential}, binding_credentials=#{binding_credentials}")

    # Load provisioned_service from dumped file
    p_service = ProvisionedService.get(service_credential['name'])
    raise "Cannot find service #{service_credential['name']}" if p_service.nil?

    p_service.run(p_service.first_start_options)
    host = get_host

    # Update credentials for the new credential
    service_credential['port']     = p_service.port
    service_credential['host']     = host
    service_credential['hostname'] = host

    binding_credentials.each_value do |value|
      v = value["credentials"]
      v['port']     = p_service.port
      v['host']     = host
      v['hostname'] = host
    end

    [service_credential, binding_credentials]
  rescue => e
    @logger.warn(e)
    p_service.delete if p_service
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
    stats = ProvisionedService.all.map do |p_service|
      stat = {}
      stat['overall'] = p_service.overall_stats
      stat['db']      = p_service.db_stats
      stat['name']    = p_service.name
      stat['version'] = p_service.version
      stat
    end

    # Get service instance status
    provisioned_instances = {}
    begin
      ProvisionedService.all.each do |p_service|
        provisioned_instances[p_service.name.to_sym] = p_service.get_healthz
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
end

class VCAP::Services::MongoDB::Node::ProvisionedService
  include DataMapper::Resource

  property :name,       String,   :key => true
  property :port,       Integer,  :unique => true
  property :password,   String,   :required => true
  # property plan is deprecated. The instances in one node have same plan.
  property :plan,       Integer,  :required => true
  property :pid,        Integer
  property :memory,     Integer
  property :admin,      String,   :required => true
  property :adminpass,  String,   :required => true
  property :db,         String,   :required => true
  property :container,  String
  property :ip,         String
  property :version,    String,   :required => false

  private_class_method :new

  # Timeout for mongo client operations, node cannot be blocked on any mongo instances.
  # Default value is 2 seconds

  MONGO_TIMEOUT = 2

  SERVICE_PORT = 27017
  PROXY_PORT   = 29017

  class << self
    def init(args)
      super(args)
      @@mongod_path        = args[:mongod_path] ? args[:mongod_path] : { args[:default_version] => 'mongod' }
      @@mongod_options     = args[:mongod_options] ? args[:mongod_options] : { args[:default_version] => '' }
      @@mongorestore_path  = args[:mongorestore_path] ? args[:mongorestore_path] : { args[:default_version] => 'mongorestore' }
      @@mongodump_path     = args[:mongodump_path] ? args[:mongodump_path] : { args[:default_version] => 'mongodump' }
      @@tar_path           = args[:tar_path] ? args[:tar_path] : 'tar'
    end

    def create(args)
      raise "Parameter missing" unless args['port'] && args['version']
      p_service           = new
      p_service.name      = args['name'] ? args['name'] : UUIDTools::UUID.random_create.to_s
      p_service.port      = args['port']
      p_service.plan      = 1
      p_service.password  = args['password'] ? args['password'] : UUIDTools::UUID.random_create.to_s
      p_service.memory    = args['memory'] if args['memory']
      p_service.admin     = args['admin'] ? args['admin'] : 'admin'
      p_service.adminpass = args['adminpass'] ? args['adminpass'] : UUIDTools::UUID.random_create.to_s
      p_service.db        = args['db'] ? args['db'] : 'db'
      p_service.version   = args['version']

      p_service.prepare_filesystem(self.max_disk)
      FileUtils.mkdir_p(p_service.data_dir)
      p_service
    end

    def import(port, dir)
      d_file = File.join(dir, 'dump_file')
      raise "No dumpfile exists" unless File.exist?(d_file)

      s_service = nil
      File.open(d_file, 'r') do |f|
        s_service = Marshal.load(f)
      end
      raise "Cannot parse dumpfile in #{d_file}" if s_service.nil?

      p_service = create('name'      => s_service.name,
                         'port'      => port,
                         'plan'      => s_service.plan,
                         'password'  => s_service.password,
                         'memory'    => s_service.memory,
                         'admin'     => s_service.admin,
                         'adminpass' => s_service.adminpass,
                         'db'        => s_service.db,
                         'version'   => s_service.version)
      FileUtils.cp_r(File.join(dir, 'data'), p_service.base_dir)
      FileUtils.rm_rf(p_service.log_dir)
      FileUtils.cp_r(File.join(dir, 'log'), p_service.log_dir)
      p_service
    end
  end

  def dump(dir)
    # dump database recorder
    d_file = File.join(dir, 'dump_file')
    File.open(d_file, 'w') do |f|
      Marshal.dump(self, f)
    end
    # dump database data/log directory
    FileUtils.cp_r(data_dir, dir)
    FileUtils.cp_r(log_dir, File.join(dir, 'log'))
  end

  def d_import(dir)
    conn = connect
    db = conn.db(self[:db])
    db.collection_names.each do |name|
      if name != 'system.users' && name != 'system.indexes'
        db[name].drop
      end
    end
    disconnect(conn)

    cmd = "#{mongorestore} -u #{self[:admin]} -p #{self[:adminpass]} -h #{self[:ip]}:#{SERVICE_PORT} #{dir}"
    output = %x{ #{mongorestore} -u #{self[:admin]} -p #{self[:adminpass]} -h #{self[:ip]}:#{SERVICE_PORT} #{dir} }
    res = $?.success?
    raise "\"#{cmd}\" failed" unless res
    true
  end

  def d_dump(dir, fake=true)
    cmd = "#{mongodump} -u #{self[:admin]} -p #{self[:adminpass]} -h #{self[:ip]}:#{SERVICE_PORT} -o #{dir}"
    return cmd if fake
    output = %x{ #{mongodump} -u #{self[:admin]} -p #{self[:adminpass]} -h #{self[:ip]}:#{SERVICE_PORT} -o #{dir} }
    res = $?.success?
    raise "\"#{cmd}\" failed" unless res
    true
  end

  def repair
    tmpdir = File.join(self.class.base_dir, "tmp", self[:name])
    FileUtils.mkdir_p(tmpdir)
    begin
      self.class.sh "#{mongod} --repair --repairpath #{tmpdir} --dbpath #{data_dir} --port #{self[:port]} --smallfiles --noprealloc", :timeout => 120
      logger.warn("Service #{self[:name]} db repair done")
    rescue => e
      logger.error("Service #{self[:name]} repair failed: #{e}")
    ensure
      FileUtils.rm_rf(tmpdir)
    end
  end

  def run(options=start_options, &post_start_block)
    # check whether the instance had been properly shutdown
    # if no, do "mongo --repair" outside of container.
    # the reason for do it outside of container:
    #  - when do repair inside container, more disk space required.
    #       (refer to http://www.mongodb.org/display/DOCS/Durability+and+Repair)
    #       Container may not have enough space to satisfy the need.
    #  - when do repair, more mem required (had experience a situation
    #       where "mongod --repair" hang with mem quota, and it resume when quota increase)
    #  - no repair if journal is enabled
    # So to avoid these situation, and make things smooth, do it outside container.
    lockfile = File.join(data_dir, "mongod.lock")
    journal_enabled = false
    if File.exist? lockfile
      case version
      when "1.8"
        journal_enabled = mongod_exe_options.match(/--journal/)
      when "2.0", "2.2"
        journal_enabled = !mongod_exe_options.match(/--nojournal/)
      end
      unless journal_enabled
        logger.warn("Service #{self[:name]} not properly shutdown, try repairing its db...")
        FileUtils.rm_f(lockfile)
        repair
      end
    end
    super
  end

  def start_options
    options = super
    options[:start_script] = {:script => "warden_service_ctl start #{adminpass} #{version} #{mongod_exe_options}", :use_spawn => true}
    options[:service_port] = PROXY_PORT
    options
  end

  def first_start_options
    options = super
    options[:post_start_script] = {:script => "#{mongo} localhost:#{SERVICE_PORT}/admin --eval 'db.addUser(\"#{self[:admin]}\", \"#{self[:adminpass]}\")'"}
    options
  end

  def stop_options
    options = super
    options[:stop_script] = {:script => "warden_service_ctl stop"}
    options
  end

  def finish_start?
    Timeout::timeout(MONGO_TIMEOUT) do
      conn = Mongo::Connection.new(ip, SERVICE_PORT)
      auth = conn.db("admin").authenticate(admin, adminpass)
      return false unless auth
    end
    true
  rescue => e
    false
  end

  def finish_first_start?
    Timeout::timeout(MONGO_TIMEOUT) do
      conn = Mongo::Connection.new(ip, SERVICE_PORT)
    end
    true
  rescue => e
    false
  end

  # diretory helper
  def data_dir
    File.join(base_dir, "data")
  end

  def add_user(username, password)
    conn = connect
    Timeout::timeout(MONGO_TIMEOUT) do
      conn.db(self[:db]).add_user(username, password)
    end
    disconnect(conn)
  end

  def remove_user(username)
    conn = connect
    Timeout::timeout(MONGO_TIMEOUT) do
      conn.db(self[:db]).remove_user(username)
    end
    disconnect(conn)
  end

  # mongodb connection
  def connect
    conn = nil
    return conn unless running?
    Timeout::timeout(MONGO_TIMEOUT) do
      conn = Mongo::Connection.new(self[:ip], SERVICE_PORT)
      auth = conn.db('admin').authenticate(self[:admin], self[:adminpass])
      raise "Authentication failed, instance: #{self[:name]}" unless auth
    end
    conn
  end

  def disconnect(conn)
    conn.close if conn
  end

  # stats helpers
  def overall_stats
    st = nil
    conn = connect
    Timeout::timeout(MONGO_TIMEOUT) do
      st = conn.db('admin').command(:serverStatus => 1)
    end
    disconnect(conn)
    return st
  rescue => e
    "Failed mongodb_overall_stats: #{e.message}, instance: #{self[:name]}"
  end

  def db_stats
    st = nil
    conn = connect
    Timeout::timeout(MONGO_TIMEOUT) do
      st = conn.db(self[:db]).stats()
    end
    disconnect(conn)
    return st
  rescue => e
    "Failed mongodb_db_stats: #{e.message}, instance: #{self[:name]}"
  end

  def get_healthz
    conn = connect
    disconnect(conn)
    "ok"
  rescue => e
    "fail"
  end

  def mongod
    @@mongod_path[version]
  end

  def mongo
    "/usr/share/mongodb/mongodb-#{version}/mongo"
  end

  def mongod_exe_options
    @@mongod_options[version]
  end

  def mongorestore
    @@mongorestore_path[version]
  end

  def mongodump
    @@mongodump_path[version]
  end
end
