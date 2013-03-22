# Copyright (c) 2009-2011 VMware, Inc.
require "erb"
require "fileutils"
require "logger"
require "pp"
require "uuidtools"
require "pg"

module VCAP
  module Services
    module Postgresql
      class Node < VCAP::Services::Base::Node
      end
    end
  end
end

require "postgresql_service/common"
require "postgresql_service/postgresql_error"
require "postgresql_service/pg_timeout"
require "postgresql_service/util"
require "postgresql_service/model"
require "postgresql_service/storage_quota"
require "postgresql_service/xlog_enforce"
require "postgresql_service/pagecache"

class VCAP::Services::Postgresql::Node

  KEEP_ALIVE_INTERVAL = 15
  STORAGE_QUOTA_INTERVAL = 1
  XLOG_ENFORCE_INTERVAL = 1

  include VCAP::Services::Postgresql::Util
  include VCAP::Services::Postgresql::Pagecache
  include VCAP::Services::Postgresql::Common
  include VCAP::Services::Postgresql

  def initialize(options)
    super(options)

    @postgresql_configs = options[:postgresql]
    @max_db_size = (options[:max_db_size] * 1024 * 1024).round
    @sys_dbs = options[:sys_dbs] || ['template0', 'template1', 'postgres']
    @max_long_query = options[:max_long_query]
    @max_long_tx = options[:max_long_tx]
    @max_db_conns = options[:max_db_conns]
    @enable_xlog_enforcer = options[:enable_xlog_enforcer]

    @base_dir = options[:base_dir]
    FileUtils.mkdir_p(@base_dir) if @base_dir

    @local_db = options[:local_db]
    @long_queries_killed = 0
    @long_tx_killed = 0
    @provision_served = 0
    @binding_served = 0
    @supported_versions = options[:supported_versions]

    # locks
    @keep_alive_lock = Mutex.new
    @kill_long_queries_lock = Mutex.new
    @kill_long_transaction_lock = Mutex.new
    @enforce_quota_lock = Mutex.new
    @enforce_xlog_lock = Mutex.new

    # connect_timeout & query_timeout
    PGDBconn.init(options)

    @use_warden = @options[:use_warden] || false
    if @use_warden
      require "postgresql_service/with_warden"
      self.class.send(:include, VCAP::Services::Postgresql::WithWarden)
      @service_start_timeout = options[:service_start_timeout] || 3
      init_ports(options[:port_range])
      pgProvisionedService.init(options)
    else
      require "postgresql_service/without_warden"
      self.class.send(:include, VCAP::Services::Postgresql::WithoutWarden)
    end
  end

  def prepare_global_connections
    @connection_mutex = Mutex.new
    @discarded_mutex = Mutex.new
    @connections = {}
    @discarded_connections = {}
  end

  def pre_send_announcement
    self.class.setup_datamapper(:default, @local_db)
    pre_send_announcement_prepare
    pre_send_announcement_internal(@options)
    check_db_consistency
    setup_timers
  end

  def announcement
    @capacity_lock.synchronize do
      { :available_capacity => @capacity,
        :capacity_unit => capacity_unit }
    end
  end

  def setup_timers
    EM.add_periodic_timer(KEEP_ALIVE_INTERVAL) { EM.defer{postgresql_keep_alive} }
    EM.add_periodic_timer(@max_long_query.to_f / 2) { EM.defer{kill_long_queries} } if @max_long_query > 0
    EM.add_periodic_timer(@max_long_tx.to_f / 2) { EM.defer{kill_long_transaction} } if @max_long_tx > 0
    EM.add_periodic_timer(STORAGE_QUOTA_INTERVAL) { EM.defer{enforce_storage_quota} }
    EM.add_periodic_timer(XLOG_ENFORCE_INTERVAL) { EM.defer{ xlog_enforce} } if @enable_xlog_enforcer
    setup_image_cache_cleaner(@options)
  end

  def all_instances_list
    pgProvisionedService.all.map{ |s| s.name }
  end

  def all_bindings_list
    res = []
    pgProvisionedService.all.each do |provisionedservice|
      provisionedservice.pgbindusers.all.each do |binduser|
        res << {
          "name" => provisionedservice.name,
          "username" => binduser.user,
          "user" => binduser.user
        }
      end
    end
    res
  end

  def check_db_consistency()
    db_list = get_db_list
    pgProvisionedService.all.each do |provisionedservice|
      db = provisionedservice.name
      provisionedservice.pgbindusers.all.each do |binduser|
        user, sys_user = binduser.user, binduser.sys_user
        if not db_list.include?([db, user]) or not db_list.include?([db, sys_user]) then
          @logger.warn("Node database inconsistent!!! db:user <#{db}:#{user}> not in PostgreSQL.")
          next
        end
      end
    end
  end

  def cleanup(provisionedservice)
    return unless provisionedservice
    name = provisionedservice.name
    port = get_inst_port(provisionedservice)
    delete_database(provisionedservice)
    init_global_connection(provisionedservice)
    provisionedservice.pgbindusers.all.each do |binduser|
      if not binduser.destroy
        @logger.error("Could not delete entry: #{binduser.errors.inspect}")
      end if binduser.saved?
    end
    if not provisionedservice.delete
      @logger.error("Could not delete entry: #{provisionedservice.errors.inspect}")
    end
    free_inst_port(port)
    delete_global_connection(name)
  end

  def provision(plan, credential=nil, version=nil)
    raise PostgresqlError.new(PostgresqlError::POSTGRESQL_INVALID_PLAN, plan) unless plan == @plan
    raise ServiceError.new(ServiceError::UNSUPPORTED_VERSION, version) unless @supported_versions.include?(version)

    if credential
      name = credential['name']
      res = pgProvisionedService.get(name)
      return gen_credential(name, res.pgbindusers[0].user, res.pgbindusers[0].password, get_inst_port(res)) if res
    end

    provisionedservice = pgProvisionedService.new
    provisionedservice.plan = 1
    provisionedservice.quota_exceeded = false
    provisionedservice.version = version

    begin
      binduser = pgBindUser.new
      if credential
        name, user, password = %w(name user password).map{ |key| credential[key] }
        provisionedservice.name = name
        binduser.user = user
        binduser.password = password
      else
        provisionedservice.name = "d-#{UUIDTools::UUID.random_create.to_s}".gsub(/-/, '')
        binduser.user = "u-#{UUIDTools::UUID.random_create.to_s}".gsub(/-/, '')
        binduser.password = "p-#{UUIDTools::UUID.random_create.to_s}".gsub(/-/, '')
      end
      binduser.sys_user = "su-#{UUIDTools::UUID.random_create.to_s}".gsub(/-/, '')
      binduser.sys_password = "sp-#{UUIDTools::UUID.random_create.to_s}".gsub(/-/, '')
      binduser.default_user = true
      provisionedservice.pgbindusers << binduser

      set_inst_port(provisionedservice, credential)

      provisionedservice.prepare

      provisionedservice.run do |instance|
        init_global_connection(instance)
        raise PostgresqlError.new(PostgresqlError::POSTGRESQL_DB_ERROR) unless create_database(instance)
        if not binduser.save
          @logger.error("Could not save entry: #{binduser.errors.inspect}")
          raise PostgresqlError.new(PostgresqlError::POSTGRESQL_LOCAL_DB_ERROR)
        end
      end
      @provision_served += 1
      return gen_credential(
                  provisionedservice.name,
                  binduser.user,
                  binduser.password,
                  get_inst_port(provisionedservice)
      )
    rescue => e
      @logger.error("Fail to provision for #{fmt_error(e)}")
      cleanup(provisionedservice) if provisionedservice
      raise e
    end
  end

  def unprovision(name, credentials)
    return if name.nil?
    @logger.info("Unprovision database:#{name} and its #{credentials ? credentials.size : 0} bindings")
    provisionedservice = pgProvisionedService.get(name)
    raise PostgresqlError.new(PostgresqlError::POSTGRESQL_CONFIG_NOT_FOUND, name) if provisionedservice.nil?
    # Delete all bindings, ignore not_found error since we are unprovision
    begin
      credentials.each{ |credential| unbind(credential)} if credentials
    rescue =>e
      # ignore
    end
    cleanup(provisionedservice)
    @logger.info("Successfully fulfilled unprovision request: #{name}")
    true
  end

  def bind(name, bind_opts, credential=nil)
    @logger.info("Bind service for db:#{name}, bind_opts = #{bind_opts}")
    binduser = nil
    begin
      provisionedservice = pgProvisionedService.get(name)
      raise PostgresqlError.new(PostgresqlError::POSTGRESQL_CONFIG_NOT_FOUND, name) unless provisionedservice
      # create new credential for binding
      if credential
        binduser = provisionedservice.pgbindusers.get(credential["user"])
        return gen_credential(
          name,
          binduser.user,
          binduser.password,
          get_inst_port(provisionedservice)
        ) if binduser
        new_user = credential["user"]
        new_password = credential["password"]
      else
        new_user = "u-#{UUIDTools::UUID.random_create.to_s}".gsub(/-/, '')
        new_password = "p-#{UUIDTools::UUID.random_create.to_s}".gsub(/-/, '')
      end
      new_sys_user = "su-#{UUIDTools::UUID.random_create.to_s}".gsub(/-/, '')
      new_sys_password = "sp-#{UUIDTools::UUID.random_create.to_s}".gsub(/-/, '')
      binduser = pgBindUser.new
      binduser.user = new_user
      binduser.password = new_password
      binduser.sys_user = new_sys_user
      binduser.sys_password = new_sys_password
      binduser.default_user = false

      instance = pgProvisionedService.get(name)
      if create_database_user(instance, binduser, provisionedservice.quota_exceeded) then
        response = gen_credential(
                    name,
                    binduser.user,
                    binduser.password,
                    get_inst_port(provisionedservice)
        )
      else
        raise PostgresqlError.new(PostgresqlError::POSTGRESQL_DB_ERROR)
      end

      provisionedservice.pgbindusers << binduser
      if not binduser.save
        @logger.error("Could not save entry: #{binduser.errors.inspect}")
        raise PostgresqlError.new(PostgresqlError::POSTGRESQL_LOCAL_DB_ERROR)
      end
      if not provisionedservice.save
        binduser.destroy
        @logger.error("Could not save entry: #{provisionedservice.errors.inspect}")
        raise PostgresqlError.new(PostgresqlError::POSTGRESQL_LOCAL_DB_ERROR)
      end

      @logger.info("Bind response: #{response.inspect}")
      @binding_served += 1
      return response
    rescue => e
      @logger.error("Fail to bind for #{fmt_error(e)}")
      delete_database_user(binduser,name) if binduser
      raise e
    end
  end

  def unbind(credential)
    return if credential.nil?
    @logger.info("Unbind service: #{credential.inspect}")
    name, user, bind_opts = %w(name user bind_opts).map{ |k| credential[k] }
    provisionedservice = pgProvisionedService.get(name)
    raise PostgresqlError.new(PostgresqlError::POSTGRESQL_CONFIG_NOT_FOUND, name) unless provisionedservice
    # validate the existence of credential, in case we delete a normal account because of a malformed credential
    global_conn = global_connection(provisionedservice)
    unless global_conn
      @logger.error("Could not connect instance #{name}.")
      return false
    end
    res = global_conn.query("SELECT count(*) from pg_authid WHERE rolname='#{user}'")
    raise PostgresqlError.new(PostgresqlError::POSTGRESQL_CRED_NOT_FOUND, credential.inspect) if res[0]['count'].to_i<=0
    unbinduser = provisionedservice.pgbindusers.get(user)
    if unbinduser != nil then
      delete_database_user(unbinduser,name)
      if not unbinduser.destroy
        @logger.error("Could not delete entry: #{unbinduser.errors.inspect}")
      end
    else
      @logger.warn("Node database inconsistent!!! user <#{user}> not in PostgreSQL.")
    end
    true
  end

  def create_database(provisionedservice)
    name, bindusers = [:name, :pgbindusers].map { |field| provisionedservice.send(field) }
    begin
      start = Time.now
      user = bindusers[0].user
      sys_user = bindusers[0].sys_user
      @logger.info("Creating: #{provisionedservice.inspect}")
      conn = setup_global_connection(provisionedservice)
      unless conn
        @logger.error("Fail to connect instance #{name} to create database")
        return false
      end
      exe_create_database(conn, name, @max_db_conns)
      if not create_database_user(provisionedservice, bindusers[0], false) then
        raise PostgresqlError.new(PostgresqlError::POSTGRESQL_DB_ERROR)
      end
      @logger.info("Done creating #{provisionedservice.inspect}. Took #{Time.now - start}.")
      true
    rescue => e
      @logger.error("Could not create database: #{fmt_error(e)}")
      false
    end
  end

  def create_database_user(instance, binduser, quota_exceeded)
    # setup parent as long as it's not the 'default user'
    name = instance.name
    parent_binduser = instance.default_user unless binduser.default_user
    parent = parent_binduser[:user] if parent_binduser

    global_conn = global_connection(instance)

    unless global_conn
      @logger.error("Fail to connect instance #{name} to create database user")
      return false
    end

    user = binduser.user
    password = binduser.password
    sys_user = binduser.sys_user
    sys_password = binduser.sys_password
    begin
      @logger.info("Creating credentials: #{user}/#{password} for database #{name}")
      exist_user = global_conn.query("select * from pg_roles where rolname = '#{user}'")
      if exist_user.num_tuples() != 0
        @logger.warn("Role: #{user} already exists")
      else
        @logger.info("Create role: #{user}/#{password}")
        if parent
          # set parent role for normal binding users
          global_conn.query("CREATE ROLE #{user} LOGIN PASSWORD '#{password}' inherit in role #{parent}")
          global_conn.query("ALTER ROLE #{user} SET ROLE=#{parent}")
        else
          global_conn.query("CREATE ROLE #{user} LOGIN PASSWORD '#{password}'")
        end
      end
      @logger.info("Create sys_role: #{sys_user}/#{sys_password}")
      global_conn.query("CREATE ROLE #{sys_user} LOGIN PASSWORD '#{sys_password}'")

      @logger.info("Grant proper privileges ...")
      db_connection = management_connection(instance, true)
      raise PGError("Fail to connect to database #{name}") unless db_connection
      db_connection.query("GRANT CONNECT ON DATABASE #{name} to #{sys_user}")
      db_connection.query("GRANT CONNECT ON DATABASE #{name} to #{user}")
      #Ignore privileges Initializing error. Log only.
      begin
        if quota_exceeded then
          # revoke create privilege on database from parent role
          # In fact, this is a noop, for the create privilege of parent user should be revoked in revoke_write_access when quota is exceeded.
          db_connection.query("REVOKE CREATE ON DATABASE #{name} FROM #{user}") unless parent
          db_connection.query("REVOKE TEMP ON DATABASE #{name} from #{user}")
          db_connection.query("REVOKE TEMP ON DATABASE #{name} from #{sys_user}")
          do_revoke_query(db_connection, user, sys_user)
        else
          # grant create privilege on database to parent role
          db_connection.query("GRANT CREATE ON DATABASE #{name} TO #{user}") unless parent
          db_connection.query("GRANT TEMP ON DATABASE #{name} to #{user}")
          db_connection.query("GRANT TEMP ON DATABASE #{name} to #{sys_user}")
          exe_grant_user_priv(db_connection)
        end
      rescue => e
        @logger.error("Could not Initialize user privileges: #{fmt_error(e)}")
      end
      db_connection.close
      true
    rescue => e
      @logger.error("Could not create database user: #{fmt_error(e)}")
      false
    end
  end

  def delete_database(provisionedservice)
    name, bindusers = [:name, :pgbindusers].map { |field| provisionedservice.send(field) }
    begin
      global_conn = global_connection(provisionedservice)
      if global_conn
        exe_drop_database(global_conn, name)
        default_binduser = provisionedservice.pgbindusers.all(:default_user => true)[0]
        if default_binduser
          # should drop objects owned by the default user, such as created schemas
          global_conn.query("DROP OWNED BY #{default_binduser.user}")
          global_conn.query("DROP OWNED BY #{default_binduser.sys_user}")
          global_conn.query("DROP ROLE IF EXISTS #{default_binduser.user}")
          global_conn.query("DROP ROLE IF EXISTS #{default_binduser.sys_user}")
        end
        true
      else
        @logger.error("Could not connect to instance #{name} to delete database.")
        false
      end
    rescue => e
      @logger.error("Could not delete database: #{fmt_error(e)}")
      false
    end
  end

  def delete_database_user(binduser,db)
    @logger.info("Delete user #{binduser.user}/#{binduser.sys_user}")
    instance = pgProvisionedService.get(db)
    db_connection = management_connection(instance, true)
    raise PGError("Fail to connect to database #{db}") unless db_connection
    begin
      db_connection.query("select pg_terminate_backend(#{pg_stat_activity_pid_field(instance.version)}) from pg_stat_activity where usename = '#{binduser.user}' or usename = '#{binduser.sys_user}'")
    rescue => e
      @logger.warn("Could not kill user session: #{e}")
    end
    #Revoke dependencies. Ignore error.
    begin
      db_connection.query("DROP OWNED BY #{binduser.user}")
      db_connection.query("DROP OWNED BY #{binduser.sys_user}")
      db_connection.query("REVOKE ALL ON ALL TABLES IN SCHEMA PUBLIC from #{binduser.user} CASCADE")
      db_connection.query("REVOKE ALL ON ALL SEQUENCES IN SCHEMA PUBLIC from #{binduser.user} CASCADE")
      db_connection.query("REVOKE ALL ON ALL FUNCTIONS IN SCHEMA PUBLIC from #{binduser.user} CASCADE")
      db_connection.query("REVOKE ALL ON ALL TABLES IN SCHEMA PUBLIC from #{binduser.sys_user} CASCADE")
      db_connection.query("REVOKE ALL ON ALL SEQUENCES IN SCHEMA PUBLIC from #{binduser.sys_user} CASCADE")
      db_connection.query("REVOKE ALL ON ALL FUNCTIONS IN SCHEMA PUBLIC from #{binduser.sys_user} CASCADE")
      db_connection.query("REVOKE ALL ON DATABASE #{db} from #{binduser.user} CASCADE")
      db_connection.query("REVOKE ALL ON SCHEMA PUBLIC from #{binduser.user} CASCADE")
      db_connection.query("REVOKE ALL ON DATABASE #{db} from #{binduser.sys_user} CASCADE")
      db_connection.query("REVOKE ALL ON SCHEMA PUBLIC from #{binduser.sys_user} CASCADE")
    rescue => e
      @logger.warn("Could not revoke user dependencies: #{e}")
    end
    db_connection.query("DROP ROLE #{binduser.user}")
    db_connection.query("DROP ROLE #{binduser.sys_user}")
    db_connection.close
    true
  rescue => e
    @logger.error("Could not delete user '#{binduser.user}': #{fmt_error(e)}")
    false
  end

  def gen_credential(name, user, passwd, port)
    host = get_host
    response = {
      "name" => name,
      "host" => host,
      "hostname" => host,
      "port" => port,
      "user" => user,
      "username" => user,
      "password" => passwd,
    }
  end

  def bind_all_creds(name, binding_creds_hash)
    binding_creds_hash.each_value do |v|
      begin
        cred = v["credentials"]
        binding_opts = v["binding_options"]
        v["credentials"] = bind(name, binding_opts, cred)
      rescue => e
        @logger.error("Error on bind_all_creds #{fmt_error(e)}")
      end
    end
  end

  # restore a given instance using backup file.
  def restore(name, backup_path)
    @logger.debug("Restore db #{name} using backup at #{backup_path}")
    instance = pgProvisionedService.get(name)
    raise PostgresqlError.new(PostgresqlError::POSTGRESQL_CONFIG_NOT_FOUND, name) unless instance
    default_user = instance.default_user
    raise "No default user for provisioned service #{name}" unless default_user

    host, port, vcap_user, vcap_pass, database, restore_bin =
      %w{host port user pass database restore_bin}.map { |opt| postgresql_config(instance)[opt] }
    reset_db(host, port, vcap_user, vcap_pass, database, instance)

    user =  default_user[:user]
    passwd = default_user[:password]
    path = File.join(backup_path, "#{name}.dump")
    restore_database(name, host, port, user, passwd, path, :restore_bin => restore_bin)
  rescue => e
    @logger.error("Error during restore: #{fmt_error(e)}")
    nil
  ensure
    FileUtils.rm_rf("#{path}.archive_list")
  end

  # kill user session & block all user
  def disable_instance(prov_cred, binding_creds)
    @logger.debug("Disable instance #{prov_cred["name"]} request.")
    name = prov_cred["name"]
    instance = pgProvisionedService.get(name)
    raise PostgresqlError.new(PostgresqlError::POSTGRESQL_CONFIG_NOT_FOUND, name) unless instance
    db_connection = management_connection(instance, true)
    raise PGError("Fail to connect to database #{name}") unless db_connection
    block_user_from_db(db_connection, instance)
    global_connection(instance).query("select pg_terminate_backend(#{pg_stat_activity_pid_field(instance.version)}) from pg_stat_activity where datname = '#{name}'")
    true
  rescue => e
    @logger.error("Error during disable_instance #{fmt_error(e)}")
    nil
  end

  # Dump db content into given path
  def dump_instance(prov_cred, binding_creds, dump_file_path)
    name = prov_cred["name"]
    @logger.debug("Dump instance #{name} request.")
    instance = pgProvisionedService.get(name)
    raise PostgresqlError.new(PostgresqlError::POSTGRESQL_CONFIG_NOT_FOUND, name) unless instance

    default_user = instance.default_user
    raise "No default user to dump instance." unless default_user
    host, port, dump_bin =  %w{host port dump_bin}.map { |opt| postgresql_config(instance)[opt] }
    user = default_user[:user]
    passwd = default_user[:password]
    dump_file = File.join(dump_file_path, "#{name}.dump")
    @logger.info("Dump instance #{name} content to #{dump_file}")
    dump_database(name, host, port, user, passwd, dump_file, :dump_bin => dump_bin)
    # dump provisioned instance object
    instance_dump_file = File.join(dump_file_path, 'instance.dump')
    File.open(instance_dump_file, 'w') do |f|
      Marshal.dump(instance, f)
    end
    true
  rescue => e
    @logger.error("Error during dump_instance #{fmt_error(e)}")
    nil
  end

  # Provision and import dump files
  # Refer to #dump_instance
  def import_instance(prov_cred, binding_creds_hash, dump_file_path, plan)
    # load provisioned instance data from dump
    stored_service = nil
    instance_dump_file = File.join(dump_file_path, 'instance.dump')
    File.open(instance_dump_file, 'r') do |f|
      stored_service = Marshal.load f
    end
    raise "Can't load instance data from #{instnace_dump_file}" unless stored_service

    version = stored_service.version
    name = prov_cred["name"]
    @logger.debug("Import instance #{name} request.")
    @logger.info("Provision an instance with plan: #{plan}, version:#{version} using data from #{prov_cred.inspect}")
    provision(plan, prov_cred, version)
    instance = pgProvisionedService.get(name)
    raise PostgresqlError.new(PostgresqlError::POSTGRESQL_CONFIG_NOT_FOUND, name) unless instance
    bind_all_creds(name, binding_creds_hash)
    default_user = instance.default_user
    raise "No default user to import instance" unless default_user
    host, port, restore_bin =  %w{host port restore_bin}.map { |opt| postgresql_config(instance)[opt] }
    user = default_user[:user]
    passwd = default_user[:password]
    import_file = File.join(dump_file_path, "#{name}.dump")
    @logger.info("Import data from #{import_file} to database #{name}")
    args = [name, host, port, user, passwd, import_file, { :restore_bin => restore_bin }]
    archive_list(*args)
    restore_database(*args)
  rescue => e
    @logger.error("Error during import_instance #{fmt_error(e)}")
    nil
  end

  def enable_instance(prov_cred, binding_creds_hash)
    name = prov_cred["name"]
    @logger.debug("Enable instance #{name} request.")
    instance = pgProvisionedService.get(name)
    raise PostgresqlError.new(PostgresqlError::POSTGRESQL_CONFIG_NOT_FOUND, name) unless instance
    db_connection = management_connection(instance, true)
    raise PGError("Fail to connect to database #{name}") unless db_connection
    unblock_user_from_db(db_connection, instance)
    true
  rescue => e
    @logger.error("Error during enable_instance #{fmt_error(e)}")
    nil
  end

  def update_instance(prov_cred, binding_creds_hash)
    @logger.debug("Update instance #{prov_cred["name"]} handles request.")
    prov_cred = gen_credential(
                  prov_cred["name"],
                  prov_cred["user"],
                  prov_cred["password"],
                  prov_cred["port"]
                )
    binding_creds_hash.each_value do |v|
      v["credentials"] = gen_credential(
                          prov_cred["name"],
                          v["credentials"]["username"],
                          v["credentials"]["password"],
                          v["credentials"]["port"]
                        )
    end
    [prov_cred, binding_creds_hash]
  rescue => e
    @logger.error("Error during update_instance #{fmt_error(e)}")
    []
  end

  def varz_details()
    varz = super
    # db stat
    varz[:db_stat] = get_db_stat
    varz[:max_capacity] = @max_capacity
    varz[:available_capacity] = @capacity
    # how many long queries and long txs are killed.
    varz[:long_queries_killed] = @long_queries_killed
    varz[:long_transactions_killed] = @long_tx_killed
    # how many provision/binding operations since startup.
    varz[:provision_served] = @provision_served
    varz[:binding_served] = @binding_served
    # get instances status
    varz[:instances] = {}
    begin
      pgProvisionedService.all.each do |instance|
        varz[:instances][instance.name.to_sym] = get_status(instance)
      end
    rescue => e
      @logger.error("Error get instance list: #{fmt_error(e)}")
    end
    varz
  rescue => e
    @logger.warn("Error during generate varz: #{fmt_error(e)}")
    {}
  end

  def get_status(instance)
    res = 'ok'
    host, port, name = %w{host port name}.map { |opt| postgresql_config(instance)[opt] }
    begin
      if instance.pgbindusers.empty? || instance.pgbindusers[0].nil?
        @logger.warn('instance without binding?!')
        res = 'fail'
      else
        user = instance.pgbindusers[0].user
        password = instance.pgbindusers[0].password
        conn = postgresql_connect(host, user, password, port, name, :quick => true)
        return 'fail' unless conn
        conn.query('select current_timestamp')
      end
    rescue => e
      @logger.warn("Error get current timestamp: #{e}")
      res = 'fail'
    ensure
      ignore_exception { conn.close if conn }
    end
    res
  end

  def add_discarded_connection(name, conn)
    unless PGDBconn.async?
      ignore_exception { conn.close if conn }
      return
    end
    @discarded_mutex.synchronize do
      @discarded_connections[name] = Array.new unless @discarded_connections[name]
      @discarded_connections[name] << conn
    end
  end

  def close_discarded_connections
    return unless PGDBconn.async?
    # try to clean the discarded connections
    @discarded_mutex.synchronize do
      to_delete = []

      @discarded_connections.each do |name, conns|
        if !conns || conns.empty?
            to_delete << name
            next
        end

        closed_conn_num = 0
        conns.each do |conn|
          unless conn
            closed_conn_num += 1
            next
          end
          begin
            ac = conn.conn_mutex.try_lock
            if ac
              ignore_exception{ conn.close if conn }
              closed_conn_num += 1
            end
          ensure
            conn.conn_mutex.unlock if ac
          end
        end

        if closed_conn_num == conns.size
          to_delete << name
          conns.clear
        end
      end

      to_delete.each do |name|
        @discarded_connections.delete(name)
      end
    end
  end

end

