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
require "postgresql_service/util"
require "postgresql_service/model"
require "postgresql_service/storage_quota"
require "postgresql_service/postgresql_error"

class VCAP::Services::Postgresql::Node

  KEEP_ALIVE_INTERVAL = 15
  STORAGE_QUOTA_INTERVAL = 1

  include VCAP::Services::Postgresql::Util
  include VCAP::Services::Postgresql::Common
  include VCAP::Services::Postgresql

  def initialize(options)
    super(options)
    @options = options.dup

    @postgresql_config = options[:postgresql]
    @max_db_size = ((options[:max_db_size] + options[:db_size_overhead]) * 1024 * 1024).round
    @max_long_query = options[:max_long_query]
    @max_long_tx = options[:max_long_tx]
    @max_db_conns = options[:max_db_conns]

    @base_dir = options[:base_dir]
    FileUtils.mkdir_p(@base_dir) if @base_dir

    @local_db = options[:local_db]
    @restore_bin = options[:restore_bin]
    @dump_bin = options[:dump_bin]

    @long_queries_killed = 0
    @long_tx_killed = 0
    @provision_served = 0
    @binding_served = 0
    @supported_versions = ["9.0"]
    @use_warden = @options[:use_warden] || false
    if @use_warden
      require "postgresql_service/with_warden"
      extend VCAP::Services::Postgresql::WithWarden
      @service_start_timeout = options[:service_start_timeout] || 3
      init_ports(options[:port_range])
      pgProvisionedService.init(options)
    else
      require "postgresql_service/without_warden"
      extend VCAP::Services::Postgresql::WithoutWarden
    end
  end

  def self.pgProvisionedServiceClass(use_warden)
    if use_warden
      VCAP::Services::Postgresql::Node::Wardenprovisionedservice
    else
      VCAP::Services::Postgresql::Node::Provisionedservice
    end
  end

  def self.pgBindUserClass(use_warden)
    if use_warden
      VCAP::Services::Postgresql::Node::WardenBinduser
    else
      VCAP::Services::Postgresql::Node::Binduser
    end
  end

  def pre_send_announcement
    self.class.setup_datamapper(:default, @local_db)
    pre_send_announcement_prepare
    @capacity_lock.synchronize do
      pre_send_announcement_internal
    end
    check_db_consistency
    setup_timers
  end

  def announcement
    @capacity_lock.synchronize do
      { :available_capacity => @capacity,
        :capacity_unit => capacity_unit }
    end
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

  def provision(plan, credential=nil, version=nil)
    raise PostgresqlError.new(PostgresqlError::POSTGRESQL_INVALID_PLAN, plan) unless plan == @plan

    if credential
      name = credential['name']
      res = pgProvisionedService.get(name)
      return gen_credential(name, res.pgbindusers[0].user, res.pgbindusers[0].password, get_inst_port(res)) if res
    end

    provisionedservice = pgProvisionedService.new
    provisionedservice.plan = 1
    provisionedservice.quota_exceeded = false

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

      if not binduser.save
        @logger.error("Could not save entry: #{binduser.errors.inspect}")
        raise PostgresqlError.new(PostgresqlError::POSTGRESQL_LOCAL_DB_ERROR)
      end
      if not provisionedservice.save
        binduser.destroy
        @logger.error("Could not save entry: #{provisionedservice.errors.inspect}")
        raise PostgresqlError.new(PostgresqlError::POSTGRESQL_LOCAL_DB_ERROR)
      end

      provisionedservice.run

      init_global_connection(provisionedservice)

      if create_database(provisionedservice) then
        @provision_served += 1
        return gen_credential(
                    provisionedservice.name,
                    binduser.user,
                    binduser.password,
                    get_inst_port(provisionedservice)
        )
      else
        raise PostgresqlError.new(PostgresqlError::POSTGRESQL_DB_ERROR)
      end
    rescue => e
      @logger.error("Fail to provision for #{e}: #{e.backtrace.join('|')}")
      if provisionedservice
        delete_database(provisionedservice)
        binduser.destroy if binduser
        provisionedservice.delete
      end
      raise e
    end
  end

  def unprovision(name, credentials)
    return if name.nil?
    @logger.info("Unprovision database:#{name} and its #{credentials.size} bindings")
    provisionedservice = pgProvisionedService.get(name)
    raise PostgresqlError.new(PostgresqlError::POSTGRESQL_CONFIG_NOT_FOUND, name) if provisionedservice.nil?
    # Delete all bindings, ignore not_found error since we are unprovision
    begin
      credentials.each{ |credential| unbind(credential)} if credentials
    rescue =>e
      # ignore
    end
    delete_database(provisionedservice)
    init_global_connection(provisionedservice)
    provisionedservice.pgbindusers.all.each do |binduser|
      if not binduser.destroy
        @logger.error("Could not delete entry: #{binduser.errors.inspect}")
      end
    end
    port = get_inst_port(provisionedservice)
    if not provisionedservice.delete
      @logger.error("Could not delete entry: #{provisionedservice.errors.inspect}")
    end
    free_inst_port(port)
    delete_global_connection(name)
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

      if create_database_user(name, binduser, provisionedservice.quota_exceeded) then
        response = gen_credential(
                    name,
                    binduser.user,
                    binduser.password,
                    get_inst_port(provisionedservice)
        )
      else
        raise PostgresqlError.new(PostgresqlError::POSTGRESQL_LOCAL_DB_ERROR)
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
      @logger.error("Fail to bind for #{e}: #{e.backtrace.join('|')}")
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
      if not create_database_user(name, bindusers[0], false) then
        raise PostgresqlError.new(PostgresqlError::POSTGRESQL_LOCAL_DB_ERROR)
      end
      @logger.info("Done creating #{provisionedservice.inspect}. Took #{Time.now - start}.")
      true
    rescue PGError => e
      @logger.error("Could not create database: #{e}")
      false
    end
  end

  def create_database_user(name, binduser, quota_exceeded)
    # setup parent as long as it's not the 'default user'
    instance = pgProvisionedService.get(name)
    parent_binduser = instance.default_user unless binduser.default_user
    parent = parent_binduser[:user] if parent_binduser

    global_conn = global_connection(instance)

    unless global_conn
      @logger.error("Fail to connect instance #{name} to create database user")
      return true
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
          # revoke create privilege on database to parent role
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
      rescue PGError => e
        @logger.error("Could not Initialize user privileges: #{e}")
      end
      db_connection.close
      true
    rescue PGError => e
      @logger.error("Could not create database user: #{e}")
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
    rescue PGError => e
      @logger.error("Could not delete database: #{e}")
      false
    end
  end

  def delete_database_user(binduser,db)
    @logger.info("Delete user #{binduser.user}/#{binduser.sys_user}")
    db_connection = management_connection(db, true)
    raise PGError("Fail to connect to database #{db}") unless db_connection
    begin
      db_connection.query("select pg_terminate_backend(procpid) from pg_stat_activity where usename = '#{binduser.user}' or usename = '#{binduser.sys_user}'")
    rescue PGError => e
      @logger.warn("Could not kill user session: #{e}")
    end
    #Revoke dependencies. Ignore error.
    begin
      db_connection.query("DROP OWNED BY #{binduser.user}")
      db_connection.query("DROP OWNED BY #{binduser.sys_user}")
      if pg_version(db_connection) == '9'
        db_connection.query("REVOKE ALL ON ALL TABLES IN SCHEMA PUBLIC from #{binduser.user} CASCADE")
        db_connection.query("REVOKE ALL ON ALL SEQUENCES IN SCHEMA PUBLIC from #{binduser.user} CASCADE")
        db_connection.query("REVOKE ALL ON ALL FUNCTIONS IN SCHEMA PUBLIC from #{binduser.user} CASCADE")
        db_connection.query("REVOKE ALL ON ALL TABLES IN SCHEMA PUBLIC from #{binduser.sys_user} CASCADE")
        db_connection.query("REVOKE ALL ON ALL SEQUENCES IN SCHEMA PUBLIC from #{binduser.sys_user} CASCADE")
        db_connection.query("REVOKE ALL ON ALL FUNCTIONS IN SCHEMA PUBLIC from #{binduser.sys_user} CASCADE")
      else
        queries = db_connection.query("select 'REVOKE ALL ON '||tablename||' from #{binduser.user} CASCADE;' as query_to_do from pg_tables where schemaname = 'public'")
        queries.each do |query_to_do|
          db_connection.query(query_to_do['query_to_do'].to_s)
        end
        queries = db_connection.query("select 'REVOKE ALL ON SEQUENCE '||relname||' from #{binduser.user} CASCADE;' as query_to_do from pg_class where relkind = 'S'")
        queries.each do |query_to_do|
          db_connection.query(query_to_do['query_to_do'].to_s)
        end
        queries = db_connection.query("select 'REVOKE ALL ON '||tablename||' from #{binduser.sys_user} CASCADE;' as query_to_do from pg_tables where schemaname = 'public'")
        queries.each do |query_to_do|
          db_connection.query(query_to_do['query_to_do'].to_s)
        end
        queries = db_connection.query("select 'REVOKE ALL ON SEQUENCE '||relname||' from #{binduser.sys_user} CASCADE;' as query_to_do from pg_class where relkind = 'S'")
        queries.each do |query_to_do|
          db_connection.query(query_to_do['query_to_do'].to_s)
        end
      end
      db_connection.query("REVOKE ALL ON DATABASE #{db} from #{binduser.user} CASCADE")
      db_connection.query("REVOKE ALL ON SCHEMA PUBLIC from #{binduser.user} CASCADE")
      db_connection.query("REVOKE ALL ON DATABASE #{db} from #{binduser.sys_user} CASCADE")
      db_connection.query("REVOKE ALL ON SCHEMA PUBLIC from #{binduser.sys_user} CASCADE")
    rescue PGError => e
      @logger.warn("Could not revoke user dependencies: #{e}")
    end
    db_connection.query("DROP ROLE #{binduser.user}")
    db_connection.query("DROP ROLE #{binduser.sys_user}")
    db_connection.close
    true
  rescue PGError => e
    @logger.error("Could not delete user '#{binduser.user}': #{e}")
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
        @logger.error("Error on bind_all_creds #{e}")
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

    host, port, vcap_user, vcap_pass =  %w{host port user pass}.map { |opt| postgresql_config(instance)[opt] }
    reset_db(host, port, vcap_user, vcap_pass, name, instance)

    user =  default_user[:user]
    passwd = default_user[:password]
    path = File.join(backup_path, "#{name}.dump")
    archive_list(path, { :restore_bin => @restore_bin })

    cmd = "#{@restore_bin} -h #{host} -p #{port} -U #{user} -L #{path}.archive_list -d #{name} #{path}"
    o, e, s = exe_cmd(cmd)
    s.exitstatus == 0
  rescue => e
    @logger.error("Error during restore: #{e}")
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
    global_connection(instance).query("select pg_terminate_backend(procpid) from pg_stat_activity where datname = '#{name}'")
    true
  rescue => e
    @logger.error("Error during disable_instance #{e}")
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
    host, port =  %w{host port}.map { |opt| postgresql_config(instance)[opt] }
    user = default_user[:user]
    passwd = default_user[:password]
    dump_file = File.join(dump_file_path, "#{name}.dump")
    @logger.info("Dump instance #{name} content to #{dump_file}")
    cmd = "#{@dump_bin} -Fc -h #{host} -p #{port} -U #{user} -f #{dump_file} #{name}"
    o, e, s = exe_cmd(cmd)
    return s.exitstatus == 0
  rescue => e
    @logger.error("Error during dump_instance #{e}")
    nil
  end

  # Provision and import dump files
  # Refer to #dump_instance
  def import_instance(prov_cred, binding_creds_hash, dump_file_path, plan)
    name = prov_cred["name"]
    @logger.debug("Import instance #{name} request.")
    @logger.info("Provision an instance with plan: #{plan} using data from #{prov_cred.inspect}")
    provision(plan, prov_cred)
    instance = pgProvisionedService.get(name)
    raise PostgresqlError.new(PostgresqlError::POSTGRESQL_CONFIG_NOT_FOUND, name) unless instance
    bind_all_creds(name, binding_creds_hash)
    default_user = instance.default_user
    raise "No default user to import instance" unless default_user
    host, port =  %w{host port}.map { |opt| postgresql_config(instance)[opt] }
    user = default_user[:user]
    passwd = default_user[:password]
    import_file = File.join(dump_file_path, "#{name}.dump")
    @logger.info("Import data from #{import_file} to database #{name}")
    archive_list(import_file, { :restore_bin => @restore_bin })
    cmd = "#{@restore_bin} -h #{host} -p #{port} -U #{user} -d #{name} -L #{import_file}.archive_list #{import_file}"
    o, e, s = exe_cmd(cmd)
    return s.exitstatus == 0
  rescue => e
    @logger.error("Error during import_instance #{e}")
    nil
  ensure
    FileUtils.rm_rf("#{import_file}.archive_list")
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
    @logger.error("Error during enable_instance #{e}")
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
    @logger.error("Error during update_instance #{e}")
    []
  end

  def varz_details()
    varz = {}
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
      @logger.error("Error get instance list: #{e}")
    end
    varz
  rescue => e
    @logger.warn("Error during generate varz: #{e}")
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
        conn = PGconn.connect(host, port, nil, nil, name,
          instance.pgbindusers[0].user, instance.pgbindusers[0].password)
        conn.query('select current_timestamp')
      end
    rescue => e
      @logger.warn("Error get current timestamp: #{e}")
      res = 'fail'
    ensure
      begin
        conn.close if conn
      rescue => e1
        #ignore
      end
    end
    res
  end

end
