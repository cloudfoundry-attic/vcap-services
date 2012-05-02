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
        class ProvisionedService
        end
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

  def self.sh(*args)
    options =
      if args[-1].respond_to?(:to_hash)
        args.pop.to_hash
      else
        {}
      end

    skip_raise = options.delete(:raise) == false
    options = { :timeout => 5.0, :max => 1024 * 1024 }.merge(options)

    status = []
    out_buf = ''
    err_buf = ''
    begin
      pid, iwr, ord, erd = POSIX::Spawn::popen4(*args)
      status = Timeout::timeout(options[:timeout]) do
        Process.waitpid2(pid)
      end
      out_buf += ord.read
      err_buf += erd.read
    rescue => e
      Process.kill("TERM", pid) if pid
      Process.detach(pid)
      @@logger.error("sh #{args} timeout: \nstdout: \n#{out_buf}\nstderr: \n#{err_buf}")
      raise e
    end

    if status[1].exitstatus != 0
      @@logger.error("sh #{args} failed: \nstdout: \n#{out_buf}\nstderr: \n#{err_buf}")
      raise "cmds #{args} failed" unless skip_raise
    else
      @@logger.info("sh #{args} success: \nstdout: \n#{out_buf}\nstderr: \n#{err_buf}")
    end
    status[1].exitstatus
  end

  def self.logger
    @@logger
  end

  def initialize(options)
    super(options)
    @@logger = options[:logger]
    ProvisionedService.init(options)
    @free_ports = options[:port_range].to_a
    @mutex = Mutex.new
  end

  def new_port(port=nil)
    @mutex.synchronize do
      raise "No ports available." if @free_ports.empty?
      return @free_ports.shift if port.nil?
      raise "port #{port} is already taken!" unless @free_ports.include?(port)
      @free_ports.delete(port)
      port
    end
  end

  def free_port(port)
    @mutex.synchronize do
      raise "port #{port} already freed!" if @free_ports.include?(port)
      @free_ports << port
    end
  end

  def del_port(port)
    @mutex.synchronize do
      @free_ports.delete(port)
    end
  end

  def port_occupied?(port)
    begin
      TCPSocket.open('localhost', port).close
      return true
    rescue => e
      return false
    end
  end

  def pre_send_announcement
    @capacity_lock.synchronize do
      ProvisionedService.all.each do |p_service|
        @capacity -= capacity_unit
        del_port(p_service.port)
        if p_service.running? then
          @logger.warn("Service #{p_service.name} already listening on port #{p_service.port}")
          next
        end

        unless p_service.data_dir?
          @logger.warn("Service #{p_service.name} in local DB, but not in file system")
          next
        end

        if p_service.image_file?
          unless p_service.image_mounted?
            # for case where VM rebooted
            @logger.info("Service #{p_service.name} mounting data file")
            sh "mount -n -o loop #{p_service.image_file} #{p_service.data_dir}"
          end
        else
          @logger.warn("Service #{p_service.name} need migration to quota")
          p_service.to_loopfile
        end

        begin
          p_service.run
        rescue => e
          @logger.error("Error starting service #{p_service.name}: #{e}")
          p_service.stop
        end
      end
    end
  end

  def shutdown
    super
    @logger.info("Shutting down instances..")
    ProvisionedService.all.each do |p_service|
      @logger.debug("Try to terminate mongod container:#{p_service.pid}")
      p_service.stop if p_service.running?
    end
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

  def provision(plan, credential = {})
    @logger.info("Provision request: plan=#{plan}")
    credential = {} if credential.nil?
    credential['plan'] = plan
    credential['port'] = new_port(credential['port'])
    p_service = ProvisionedService.create(credential)
    p_service.run

    username = credential['username'] ? credential['username'] : UUIDTools::UUID.random_create.to_s
    password = credential['password'] ? credential['username'] : UUIDTools::UUID.random_create.to_s
    p_service.add_admin(p_service.admin, p_service.adminpass)
    p_service.add_user(p_service.admin, p_service.adminpass)
    p_service.add_user(username, password)

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
    port = p_service.port
    raise ServiceError.new(ServiceError::NOT_FOUND, name) if p_service.nil?
    raise "Could not cleanup service #{p_service.errros.inspect}" unless p_service.delete
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
      "db"       => p_service.db
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

    p_service.run
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
  property :plan,       Enum[:free], :required => true
  property :pid,        Integer
  property :memory,     Integer
  property :admin,      String,   :required => true
  property :adminpass,  String,   :required => true
  property :db,         String,   :required => true
  property :container,  String
  property :ip,         String

  private_class_method :new

  # Timeout for mongo client operations, node cannot be blocked on any mongo instances.
  # Default value is 2 seconds

  MONGO_TIMEOUT = 2

  class << self
    def init(args)
      raise "Parameter :base_dir missing" unless args[:base_dir]
      raise "Parameter :mongod_log_dir missing" unless args[:mongod_log_dir]
      raise "Parameter :image_dir missing" unless args[:image_dir]
      raise "Parameter :local_db missing" unless args[:local_db]
      @@mongod_path = args[:mongod_path] ? args[:mongod_path] : 'mongod'
      @@mongorestore_path = args[:mongorestore_path] ? args[:mongorestore_path] : 'mongorestore'
      @@mongodump_path    = args[:mongodump_path] ? args[:mongodump_path] : 'mongodump'
      @@tar_path          = args[:tar_path] ? args[:tar_path] : 'tar'
      @@base_dir          = args[:base_dir]
      @@log_dir           = args[:mongod_log_dir]
      @@image_dir         = args[:image_dir]
      @@max_db_size       = args[:max_db_size] ? args[:max_db_size] : 128
      @@warden_client     = Warden::Client.new("/tmp/warden.sock")
      @@warden_client.connect
      @@warden_lock = Mutex.new
      DataMapper.setup(:default, args[:local_db])
      DataMapper::auto_upgrade!
      FileUtils.mkdir_p(@@base_dir)
      FileUtils.mkdir_p(@@log_dir)
      FileUtils.mkdir_p(@@image_dir)
    end

    def create(args)
      raise "Parameter missing" unless args['port']
      p_service           = new
      p_service.name      = args['name'] ? args['name'] : UUIDTools::UUID.random_create.to_s
      p_service.port      = args['port']
      p_service.plan      = args['plan'] ? args['plan'] : 'free'
      p_service.password  = args['password'] ? args['password'] : UUIDTools::UUID.random_create.to_s
      p_service.memory    = args['memory'] if args['memory']
      p_service.admin     = args['admin'] ? args['admin'] : 'admin'
      p_service.adminpass = args['adminpass'] ? args['adminpass'] : UUIDTools::UUID.random_create.to_s
      p_service.db        = args['db'] ? args['db'] : 'db'

      raise "Cannot save provision service" unless p_service.save!

      FileUtils.rm_rf(p_service.data_dir)
      FileUtils.rm_rf(p_service.log_dir)
      FileUtils.rm_rf(p_service.image_file)
      FileUtils.mkdir_p(p_service.data_dir)
      FileUtils.mkdir_p(p_service.log_dir)
      Node.sh "dd if=/dev/null of=#{p_service.image_file} bs=1M seek=#{@@max_db_size}"
      Node.sh "mkfs.ext4 -q -F -O \"^has_journal,uninit_bg\" #{p_service.image_file}"
      Node.sh "mount -n -o loop #{p_service.image_file} #{p_service.data_dir}"
      FileUtils.mkdir_p(File.join(p_service.data_dir, 'data'))
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
                         'db'        => s_service.db)
      FileUtils.cp_r(File.join(dir, 'data'), p_service.data_dir)
      FileUtils.rm_rf(p_service.log_dir)
      FileUtils.cp_r(File.join(dir, 'log'), p_service.log_dir)
      p_service
    end
  end

  def delete
    # stop container
    stop if running?
    # delete log and service directory
    Node.sh "umount #{data_dir}"
    FileUtils.rm_rf(image_file)
    FileUtils.rm_rf(data_dir)
    FileUtils.rm_rf(log_dir)
    # delete recorder
    destroy!
  end

  def image_mounted?
    mounted = false
    File.open("/proc/mounts", mode="r") do |f|
      f.each do |w|
        if Regexp.new(data_dir) =~ w
          mounted = true
          break
        end
      end
    end
    mounted
  end

  def to_loopfile
    FileUtils.mv(data_dir, data_dir+"_bak")
    FileUtils.mkdir_p(data_dir)
    Node.sh "A=`du -sm #{data_dir+"_bak"} | awk '{ print $1 }'`;A=$((A+32));if [ $A -lt #{@@max_db_size} ]; then A=#{@@max_db_size}; fi;dd if=/dev/null of=#{image_file} bs=1M seek=$A"
    Node.sh "mkfs.ext4 -q -F -O \"^has_journal,uninit_bg\" #{image_file}"
    Node.sh "mount -n -o loop #{image_file} #{data_dir}"
    FileUtils.cp_r(File.join(data_dir+"_bak", 'data'), data_dir)
  end

  def dump(dir)
    # dump database recorder
    d_file = File.join(dir, 'dump_file')
    File.open(d_file, 'w') do |f|
      Marshal.dump(self, f)
    end
    # dump database data/log directory
    FileUtils.cp_r(File.join(data_dir, 'data'), dir)
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

    output = %x{ #{@@mongorestore_path} -u #{self[:admin]} -p #{self[:adminpass]} -h #{self[:ip]}:27017 #{dir} }
    res = $?.success?
    raise "\"#{cmd}\" failed" unless res
    true
  end

  def d_dump(dir, fake=true)
    cmd = "#{@@mongodump_path} -u #{self[:admin]} -p #{self[:adminpass]} -h #{self[:ip]}:27017 -o #{dir}"
    return cmd if fake
    output = %x{ #{@@mongodump_path} -u #{self[:admin]} -p #{self[:adminpass]} -h #{self[:ip]}:27017 -o #{dir} }
    res = $?.success?
    raise "\"#{cmd}\" failed" unless res
    true
  end

  def repair
    tmpdir = File.join("/var/vcap/store/tmp", self[:name])
    FileUtils.mkdir_p(tmpdir)
    begin
      Node.sh "#{@@mongod_path} --repair --repairpath #{tmpdir} --dbpath #{File.join(data_dir, "data")} --port #{self[:port]}", :timeout => 120
      Node.logger.warn("Service #{self[:name]} db repair done")
    rescue => e
      Node.logger.error("Service #{self[:name]} repair failed: #{e}")
    ensure
      FileUtils.rm_rf(tmpdir)
    end
  end

  # mongod control
  def running?
    if (self[:container] == '')
      return false
    else
      @@warden_lock.synchronize do
        @@warden_client.call(["info", self[:container]])
      end
      return true
    end
  rescue => e
    return false
  end

  def stop(timeout=5,sig=:SIGTERM)
    unmapping_port(self[:ip], self[:port])
    unless self[:container] == ''
      @@warden_lock.synchronize do
        @@warden_client.call(["stop", self[:container]])
        @@warden_client.call(["destroy", self[:container]])
      end
    end
    self[:container] = ''
    save
    true
  end

  def run
    # check whether the instance had been properly shutdown
    # if no, do "mongo --repair" outside of container.
    # the reason for do it outside of container:
    #  - when do repair inside container, more disk space required.
    #       (refer to http://www.mongodb.org/display/DOCS/Durability+and+Repair)
    #       Container may not have enough space to satisfy the need.
    #  - when do repair, more mem required (had experience a situation
    #       where "mongod --repair" hang with mem quota, and it resume when quota increase)
    # So to avoid these situation, and make things smooth, do it outside container.
    lockfile = File.join(data_dir, "data", "mongod.lock")
    if File.size?(lockfile)
      Node.logger.warn("Service #{self[:name]} not properly shutdown, try repairing its db...")
      FileUtils.rm_f(lockfile)
      repair
    end

    @@warden_lock.synchronize do
      req = ["create", {"bind_mounts" => [[File.join(data_dir, 'data'), "/store/data", {"mode" => "rw"}],
                                          [log_dir, "/store/log", {"mode" => "rw"}]]}]
      self[:container] = @@warden_client.call(req)
      req = ["info", self[:container]]
      info = @@warden_client.call(req)
      self[:ip] = info["container_ip"]
    end
    save!
    mapping_port(self[:ip], self[:port])
  end

  def mapping_port(ip, port)
    rule = [ "--protocol tcp",
             "--dport #{port}",
             "--jump DNAT",
             "--to-destination #{ip}:27017" ]
    Node.sh "iptables -t nat -A PREROUTING #{rule.join(" ")}"
  end

  def unmapping_port(ip, port)
    rule = [ "--protocol tcp",
             "--dport #{port}",
             "--jump DNAT",
             "--to-destination #{ip}:27017" ]
    Node.sh "iptables -t nat -D PREROUTING #{rule.join(" ")}"
  end

  # diretory helper
  def image_file
    File.join(@@image_dir, "#{self[:name]}.img")
  end

  def data_dir
    File.join(@@base_dir, self[:name])
  end

  def log_dir
    File.join(@@log_dir, self[:name])
  end

  def image_file?
    File.exists?(image_file)
  end

  def data_dir?
    Dir.exists?(data_dir)
  end

  def log_dir?
    Dir.exists?(log_dir)
  end

  # user management helper
  def add_admin(username, password)
    @@warden_lock.synchronize do
      @@warden_client.call(["run", self[:container], "mongo localhost:27017/admin --eval 'db.addUser(\"#{username}\", \"#{password}\")'"])
    end
  rescue => e
    raise "Could not add admin user \'#{username}\'"
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
      conn = Mongo::Connection.new(self[:ip], '27017')
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
end
