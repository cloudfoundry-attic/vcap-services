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
  end

  def pre_send_announcement
    Node.setup_datamapper(:default, @local_db)
    @connection = postgresql_connect(@postgresql_config["host"],@postgresql_config["user"],@postgresql_config["pass"],@postgresql_config["port"],@postgresql_config["database"])
    check_db_consistency()

    @capacity_lock.synchronize do
      Provisionedservice.all.each do |provisionedservice|
        migrate_instance provisionedservice
        @capacity -= capacity_unit
      end
    end

    EM.add_periodic_timer(KEEP_ALIVE_INTERVAL) {postgresql_keep_alive}
    EM.add_periodic_timer(@max_long_query.to_f / 2) {kill_long_queries} if @max_long_query > 0
    EM.add_periodic_timer(@max_long_tx.to_f / 2) {kill_long_transaction} if @max_long_tx > 0
    EM.add_periodic_timer(STORAGE_QUOTA_INTERVAL) {enforce_storage_quota}
  end

  # This method performs whatever 'migration' (upgrade/downgrade)
  # steps are required due to incompatible code changes.  There is no
  # concept of an instance's "version", so migration code may need to
  # inspect the instance to determine what migrations are required.
  def migrate_instance(provisionedservice)
    # Services-r7 and earlier had a bug whereby database objects were
    # owned by the users created by bind operations, which caused
    # various problems (eg these objects were discarded on an 'unbind'
    # operation, only the original creator of an object could modify
    # it, etc).  Services-r8 fixes this problem by granting all 'children'
    # bind users to a 'parent' role, and setting all 'children' bind users'
    # default connection session to be 'parent' role's configuration parameter.
    # But this fix only works for newly created users and objects, so we
    # need to call this object-ownership method to migration 'old' users
    # and objects. we don't need to worry about calling it more than once
    # because doing so is harmless.
    manage_object_ownership(provisionedservice.name)
  end

  def get_expected_children(name)
    # children according to Provisionedservice
    children = Provisionedservice.get(name).bindusers.all(:default_user => false)
    children = children.map { |child| child.user } + children.map { |child| child.sys_user }
    children
  end

  def get_actual_children(connection, name, parent)
    # children according to postgres itself
    children = []
    rows = connection.query("SELECT datacl FROM pg_database WHERE datname='#{name}'")
    raise "Can't get datacl" if rows.nil? || rows.num_tuples < 1
    datacl = rows[0]['datacl']
    # a typical pg_database.datacl value:
    # {vcap=CTc/vcap,suf4f57864f519412b82ffd0b75d02dcd1=c/vcap,u2e47852f15544536b2f69c0f72052847=c/vcap,su76f8095858e742d1954544c722b277f8=c/vcap,u02b45d2974644895b1b03a92749250b2=c/vcap,su7950e259bbe946328ba4e3540c141f4b=c/vcap,uaf8982bc76324c6e9a09596fa1e57fc3=c/vcap}
    raise "Datacl is nil/deformed" if datacl.nil? || datacl.length < 2
    nonchildren = [@postgresql_config["user"], parent.user, parent.sys_user, '']
    datacl[1,datacl.length-1].split(',').each do |aclitem|
      child = aclitem.split('=')[0]
      children << child unless nonchildren.include?(child)
    end
    children
  end

  def get_unruly_children(connection, parent, children)
    # children which are not in fact children of the parent. (we don't
    # handle children that somehow have the *wrong* parent, but that
    # won't happen :-)
    query = <<-end_of_query
      SELECT rolname
      FROM pg_roles
      WHERE oid IN (
        SELECT member
        FROM pg_auth_members
        WHERE roleid IN (
          SELECT oid
          FROM pg_roles
          WHERE rolname='#{parent.user}'
        )
      );
    end_of_query
    ruly_children = connection.query(query).map { |row| row['rolname'] }
    children - ruly_children
  end

  def manage_object_ownership(name)
    # figure out which children *should* exist
    expected_children = get_expected_children name
    # optimization: the set of children we need to take action for is
    # a subset of the expected childen, so if there are no expected
    # children we can stop right now
    return if expected_children.empty?
    # the parent role
    parent = Provisionedservice.get(name).bindusers.all(:default_user => true)[0]
    # connect as the system user (not the parent or any of the
    # children) to ensure we don't have ACL problems
    connection = postgresql_connect @postgresql_config["host"], @postgresql_config["user"], @postgresql_config["pass"], @postgresql_config["port"], name
    # figure out which children *actually* exist
    actual_children = get_actual_children connection, name, parent
    # log but ignore children that aren't both expected and actually exist
    children = expected_children & actual_children
    @logger.warn "Ignoring surplus children #{actual_children-children} in #{name}" unless (actual_children-children).empty?
    @logger.warn "Ignoring missing children #{expected_children-children} in #{name}" unless (expected_children-children).empty?
    # if there are no children, then there is nothing to do
    return if children.empty?
    # ensure that all children and in fact children of their parents
    unruly_children = get_unruly_children(connection, parent, children)
    unless unruly_children.empty?
      unruly_children.each do |u_c|
        connection.query("alter role #{u_c} inherit")
        connection.query("alter role #{u_c} set role=#{parent.user}")
      end
      connection.query("GRANT #{parent.user} TO #{unruly_children.join(',')};")
      @logger.info("New children #{unruly_children} of parent #{parent.user}")
    end
    # make all current objects owned by the parent
    connection.query("REASSIGN OWNED BY #{children.join(',')} TO #{parent.user};")
  rescue => x
    @logger.warn("Exception while managing object ownership: #{x}")
  ensure
    connection.close if connection
  end

  def announcement
    @capacity_lock.synchronize do
      { :available_capacity => @capacity,
        :capacity_unit => capacity_unit }
    end
  end

  def all_instances_list
    Provisionedservice.all.map{ |s| s.name }
  end

  def all_bindings_list
    res = []
    Provisionedservice.all.each do |provisionedservice|
      provisionedservice.bindusers.all.each do |binduser|
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
    db_list = []
    @connection.query('select datname,datacl from pg_database').each{ |message|
      datname = message['datname']
      datacl = message['datacl']
      if not datacl==nil
        users = datacl[1,datacl.length-1].split(',')
        for user in users
          if user.split('=')[0].empty?
          else
            db_list.push([datname, user.split('=')[0]])
          end
        end
      end
    }
    Provisionedservice.all.each do |provisionedservice|
      db = provisionedservice.name
      provisionedservice.bindusers.all.each do |binduser|
        user, sys_user = binduser.user, binduser.sys_user
        if not db_list.include?([db, user]) or not db_list.include?([db, sys_user]) then
          @logger.warn("Node database inconsistent!!! db:user <#{db}:#{user}> not in PostgreSQL.")
          next
        end
      end
    end
  end

  def postgresql_connect(host, user, password, port, database, fail_with_nil = false)
    5.times do
      begin
        @logger.info("PostgreSQL connect: #{host}, #{port}, #{user}, #{password}, #{database} (fail_with_nil: #{fail_with_nil})")
        connect = PGconn.connect(host, port, nil, nil, database, user, password)
        version = get_postgres_version(connect)
        @logger.info("PostgreSQL server version: #{version}")
        @logger.info("Connected")
        return connect
      rescue PGError => e
        @logger.error("PostgreSQL connection attempt failed: #{host} #{port} #{database} #{user} #{password}")
        sleep(2)
      end
    end
    if fail_with_nil
      @logger.warn("PostgreSQL connection unrecoverable")
      return nil
    else
      @logger.fatal("PostgreSQL connection unrecoverable")
      shutdown
      exit
    end
  end

  #keep connection alive, and check db liveness
  def postgresql_keep_alive
    if connection_exception
      @logger.warn("PostgreSQL connection lost, trying to keep alive.")
      @connection = postgresql_connect(@postgresql_config["host"],@postgresql_config["user"],@postgresql_config["pass"],@postgresql_config["port"],@postgresql_config["database"])
    end
  end

  def is_default_bind_user(user_name)
    user = Binduser.get(user_name)
    !user.nil? && user.default_user
  end

  def kill_long_queries
    # (extract(epoch from current_timestamp) - extract(epoch from query_start)) as runtime
    # Notice: we should use current_timestamp or timeofday, the difference is that the current_timestamp only executed once at the beginning of the transaction, while dayoftime will return a text string of wall-clock time and advances during the transaction
    # Filtering the long queries in the pg statement is better than filtering using the iteration of ruby after select all activties
    process_list = @connection.query("select * from (select procpid, datname, query_start, usename, (extract(epoch from current_timestamp) - extract(epoch from query_start)) as run_time from pg_stat_activity where query_start is not NULL and usename != '#{@postgresql_config['user']}' and current_query !='<IDLE>') as inner_table  where run_time > #{@max_long_query}")
    process_list.each do |proc|
      unless is_default_bind_user(proc["usename"])
        @connection.query("select pg_terminate_backend(#{proc['procpid']})")
        @logger.info("Killed long query: user:#{proc['usename']} db:#{proc['datname']} time:#{Time.now.to_i - Time::parse(proc['query_start']).to_i} info:#{proc['current_query']}")
        @long_queries_killed += 1
      end
    end
  rescue PGError => e
    @logger.warn("PostgreSQL error: #{e}")
  end

  def kill_long_transaction
    # see kill_long_queries
    process_list = @connection.query("select * from (select procpid, datname, xact_start, usename, (extract(epoch from current_timestamp) - extract(epoch from xact_start)) as run_time from pg_stat_activity where xact_start is not NULL and usename != '#{@postgresql_config['user']}') as inner_table where run_time > #{@max_long_tx}")
    process_list.each do |proc|
      unless is_default_bind_user(proc["usename"])
        @connection.query("select pg_terminate_backend(#{proc['procpid']})")
        @logger.info("Killed long transaction: user:#{proc['usename']} db:#{proc['datname']} active_time:#{Time.now.to_i - Time::parse(proc['xact_start']).to_i}")
        @long_tx_killed += 1
      end
    end
  rescue PGError => e
    @logger.warn("PostgreSQL error: #{e}")
  end

  def provision(plan, credential=nil)
    raise PostgresqlError.new(PostgresqlError::POSTGRESQL_INVALID_PLAN, plan) unless plan == @plan
    provisionedservice = Provisionedservice.new
    provisionedservice.plan = 1

    begin
      binduser = Binduser.new
      if credential
        name, user, password = %w(name user password).map{ |key| credential[key] }
        res = Provisionedservice.get(name)
        return gen_credential(name, res.bindusers[0].user, res.bindusers[0].password) if res
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
      provisionedservice.quota_exceeded = false
      provisionedservice.bindusers << binduser
      if create_database(provisionedservice) then
        if not binduser.save
          @logger.error("Could not save entry: #{binduser.errors.inspect}")
          raise PostgresqlError.new(PostgresqlError::POSTGRESQL_LOCAL_DB_ERROR)
        end
        if not provisionedservice.save
          binduser.destroy
          @logger.error("Could not save entry: #{provisionedservice.errors.inspect}")
          raise PostgresqlError.new(PostgresqlError::POSTGRESQL_LOCAL_DB_ERROR)
        end
        response = gen_credential(provisionedservice.name, binduser.user, binduser.password)
        @provision_served += 1
        return response
      else
        raise PostgresqlError.new(PostgresqlError::POSTGRESQL_LOCAL_DB_ERROR)
      end
    rescue => e
      delete_database(provisionedservice) if provisionedservice
      raise e
    end
  end

  def unprovision(name, credentials)
    return if name.nil?
    @logger.info("Unprovision database:#{name} and its #{credentials.size} bindings")
    provisionedservice = Provisionedservice.get(name)
    raise PostgresqlError.new(PostgresqlError::POSTGRESQL_CONFIG_NOT_FOUND, name) if provisionedservice.nil?
    # Delete all bindings, ignore not_found error since we are unprovision
    begin
      credentials.each{ |credential| unbind(credential)} if credentials
    rescue =>e
      # ignore
    end
    delete_database(provisionedservice)

    provisionedservice.bindusers.all.each do |binduser|
      if not binduser.destroy
        @logger.error("Could not delete entry: #{binduser.errors.inspect}")
      end
    end
    if not provisionedservice.destroy
      @logger.error("Could not delete entry: #{provisionedservice.errors.inspect}")
    end
    @logger.info("Successfully fulfilled unprovision request: #{name}")
    true
  end

  def bind(name, bind_opts, credential=nil)
    @logger.info("Bind service for db:#{name}, bind_opts = #{bind_opts}")
    binduser = nil
    begin
      provisionedservice = Provisionedservice.get(name)
      raise PostgresqlError.new(PostgresqlError::POSTGRESQL_CONFIG_NOT_FOUND, name) unless provisionedservice
      # create new credential for binding
      if credential
        binduser = provisionedservice.bindusers.get(credential["user"])
        return gen_credential(name, binduser.user, binduser.password) if binduser
        new_user = credential["user"]
        new_password = credential["password"]
      else
        new_user = "u-#{UUIDTools::UUID.random_create.to_s}".gsub(/-/, '')
        new_password = "p-#{UUIDTools::UUID.random_create.to_s}".gsub(/-/, '')
      end
      new_sys_user = "su-#{UUIDTools::UUID.random_create.to_s}".gsub(/-/, '')
      new_sys_password = "sp-#{UUIDTools::UUID.random_create.to_s}".gsub(/-/, '')
      binduser = Binduser.new
      binduser.user = new_user
      binduser.password = new_password
      binduser.sys_user = new_sys_user
      binduser.sys_password = new_sys_password
      binduser.default_user = false

      if create_database_user(name, binduser, provisionedservice.quota_exceeded) then
        response = gen_credential(name, binduser.user, binduser.password)
      else
        raise PostgresqlError.new(PostgresqlError::POSTGRESQL_LOCAL_DB_ERROR)
      end

      provisionedservice.bindusers << binduser
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
      delete_database_user(binduser,name) if binduser
      raise e
    end
  end

  def unbind(credential)
    return if credential.nil?
    @logger.info("Unbind service: #{credential.inspect}")
    name, user, bind_opts = %w(name user bind_opts).map{ |k| credential[k] }
    provisionedservice = Provisionedservice.get(name)
    raise PostgresqlError.new(PostgresqlError::POSTGRESQL_CONFIG_NOT_FOUND, name) unless provisionedservice
    # validate the existence of credential, in case we delete a normal account because of a malformed credential
    res = @connection.query("SELECT count(*) from pg_authid WHERE rolname='#{user}'")
    raise PostgresqlError.new(PostgresqlError::POSTGRESQL_CRED_NOT_FOUND, credential.inspect) if res[0]['count'].to_i<=0
    unbinduser = provisionedservice.bindusers.get(user)
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
    name, bindusers = [:name, :bindusers].map { |field| provisionedservice.send(field) }
    begin
      start = Time.now
      user = bindusers[0].user
      sys_user = bindusers[0].sys_user
      @logger.info("Creating: #{provisionedservice.inspect}")
      exe_create_database(name)
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

  def exe_create_database(name)
    @logger.debug("Maximum connections: #{@max_db_conns}")
    @connection.query("CREATE DATABASE #{name} WITH CONNECTION LIMIT = #{@max_db_conns}")
    @connection.query("REVOKE ALL ON DATABASE #{name} FROM PUBLIC")
  end

  def exe_grant_user_priv(db_connection)
    db_connection.query("grant create on schema public to public")
    if get_postgres_version(db_connection) == '9'
      db_connection.query("grant all on all tables in schema public to public")
      db_connection.query("grant all on all sequences in schema public to public")
      db_connection.query("grant all on all functions in schema public to public")
    else
      querys = db_connection.query("select 'grant all on '||tablename||' to public;' as query_to_do from pg_tables where schemaname = 'public'")
      querys.each do |query_to_do|
        p query_to_do['query_to_do'].to_s
        db_connection.query(query_to_do['query_to_do'].to_s)
      end
      querys = db_connection.query("select 'grant all on sequence '||relname||' to public;' as query_to_do from pg_class where relkind = 'S'")
      querys.each do |query_to_do|
        db_connection.query(query_to_do['query_to_do'].to_s)
      end
    end
  end

  def create_database_user(name, binduser, quota_exceeded)
    # setup parent as long as it's not the 'default user'
    parent_binduser = Provisionedservice.get(name).bindusers.all(:default_user => true)[0] unless binduser.default_user
    parent = parent_binduser.user if parent_binduser

    user = binduser.user
    password = binduser.password
    sys_user = binduser.sys_user
    sys_password = binduser.sys_password
    begin
      @logger.info("Creating credentials: #{user}/#{password} for database #{name}")
      exist_user = @connection.query("select * from pg_roles where rolname = '#{user}'")
      if exist_user.num_tuples() != 0
        @logger.warn("Role: #{user} already exists")
      else
        @logger.info("Create role: #{user}/#{password}")
        if parent
          # set parent role for normal binding users
          @connection.query("CREATE ROLE #{user} LOGIN PASSWORD '#{password}' inherit in role #{parent}")
          @connection.query("ALTER ROLE #{user} SET ROLE=#{parent}")
        else
          @connection.query("CREATE ROLE #{user} LOGIN PASSWORD '#{password}'")
        end
      end
      @logger.info("Create sys_role: #{sys_user}/#{sys_password}")
      @connection.query("CREATE ROLE #{sys_user} LOGIN PASSWORD '#{sys_password}'")

      @logger.info("Grant proper privileges ...")
      db_connection = postgresql_connect(@postgresql_config["host"],@postgresql_config["user"],@postgresql_config["pass"],@postgresql_config["port"],name)
      db_connection.query("GRANT CONNECT ON DATABASE #{name} to #{sys_user}")
      db_connection.query("GRANT CONNECT ON DATABASE #{name} to #{user}")
      #Ignore privileges Initializing error. Log only.
      begin
        if quota_exceeded then
          do_revoke_query(db_connection, user, sys_user)
        else
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
    name, bindusers = [:name, :bindusers].map { |field| provisionedservice.send(field) }
    begin
      exe_drop_database(name)
      default_binduser = bindusers.all(:default_user => true)[0]
      @connection.query("DROP ROLE IF EXISTS #{default_binduser.user}") if default_binduser
      @connection.query("DROP ROLE IF EXISTS #{default_binduser.sys_user}") if default_binduser
      true
    rescue PGError => e
      @logger.error("Could not delete database: #{e}")
      false
    end
  end

  def exe_drop_database(name)
    @logger.info("Deleting database: #{name}")
    begin
      @connection.query("select pg_terminate_backend(procpid) from pg_stat_activity where datname = '#{name}'")
    rescue PGError => e
      @logger.warn("Could not kill database session: #{e}")
    end
    @connection.query("DROP DATABASE #{name}")
  end

  def delete_database_user(binduser,db)
    @logger.info("Delete user #{binduser.user}/#{binduser.sys_user}")
    db_connection = postgresql_connect(@postgresql_config["host"],@postgresql_config["user"],@postgresql_config["pass"],@postgresql_config["port"],db)
    begin
      db_connection.query("select pg_terminate_backend(procpid) from pg_stat_activity where usename = '#{binduser.user}' or usename = '#{binduser.sys_user}'")
    rescue PGError => e
      @logger.warn("Could not kill user session: #{e}")
    end
    #Revoke dependencies. Ignore error.
    begin
      db_connection.query("DROP OWNED BY #{binduser.user}")
      db_connection.query("DROP OWNED BY #{binduser.sys_user}")
      if get_postgres_version(db_connection) == '9'
        db_connection.query("REVOKE ALL ON ALL TABLES IN SCHEMA PUBLIC from #{binduser.user} CASCADE")
        db_connection.query("REVOKE ALL ON ALL SEQUENCES IN SCHEMA PUBLIC from #{binduser.user} CASCADE")
        db_connection.query("REVOKE ALL ON ALL FUNCTIONS IN SCHEMA PUBLIC from #{binduser.user} CASCADE")
        db_connection.query("REVOKE ALL ON ALL TABLES IN SCHEMA PUBLIC from #{binduser.sys_user} CASCADE")
        db_connection.query("REVOKE ALL ON ALL SEQUENCES IN SCHEMA PUBLIC from #{binduser.sys_user} CASCADE")
        db_connection.query("REVOKE ALL ON ALL FUNCTIONS IN SCHEMA PUBLIC from #{binduser.sys_user} CASCADE")
      else
        querys = db_connection.query("select 'REVOKE ALL ON '||tablename||' from #{binduser.user} CASCADE;' as query_to_do from pg_tables where schemaname = 'public'")
        querys.each do |query_to_do|
          db_connection.query(query_to_do['query_to_do'].to_s)
        end
        querys = db_connection.query("select 'REVOKE ALL ON SEQUENCE '||relname||' from #{binduser.user} CASCADE;' as query_to_do from pg_class where relkind = 'S'")
        querys.each do |query_to_do|
          db_connection.query(query_to_do['query_to_do'].to_s)
        end
        querys = db_connection.query("select 'REVOKE ALL ON '||tablename||' from #{binduser.sys_user} CASCADE;' as query_to_do from pg_tables where schemaname = 'public'")
        querys.each do |query_to_do|
          db_connection.query(query_to_do['query_to_do'].to_s)
        end
        querys = db_connection.query("select 'REVOKE ALL ON SEQUENCE '||relname||' from #{binduser.sys_user} CASCADE;' as query_to_do from pg_class where relkind = 'S'")
        querys.each do |query_to_do|
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

  def gen_credential(name, user, passwd)
    host = get_host
    response = {
      "name" => name,
      "host" => host,
      "hostname" => host,
      "port" => @postgresql_config['port'],
      "user" => user,
      "username" => user,
      "password" => passwd,
    }
  end

  def get_postgres_version(db_connection)
    version = db_connection.query("select version()")
    reg = /([0-9.]{5})/
    return version[0]['version'].scan(reg)[0][0][0]
  end

  def block_user_from_db(db_connection, service)
    name = service.name
    default_user = service.bindusers.all(:default_user => true)[0]
    service.bindusers.all.each do |binduser|
      if binduser.default_user == false
        db_connection.query("revoke #{default_user.user} from #{binduser.user}")
        db_connection.query("revoke connect on database #{name} from #{binduser.user}")
        db_connection.query("revoke connect on database #{name} from #{binduser.sys_user}")
      end
    end
  end

  def unblock_user_from_db(db_connection, service)
    name = service.name
    default_user = service.bindusers.all(:default_user => true)[0]
    service.bindusers.all.each do |binduser|
      if binduser.default_user == false
        db_connection.query("GRANT CONNECT ON DATABASE #{name} to #{binduser.user}")
        db_connection.query("GRANT CONNECT ON DATABASE #{name} to #{binduser.sys_user}")
        db_connection.query("GRANT #{default_user.user} to #{binduser.user}")
      end
    end
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
    service = Provisionedservice.get(name)
    raise PostgresqlError.new(PostgresqlError::POSTGRESQL_CONFIG_NOT_FOUND, name) unless service
    default_user = service.bindusers.all(:default_user => true)[0]
    raise "No default user for provisioned service #{name}" unless default_user
    reset_db(@postgresql_config['host'], @postgresql_config['port'], @postgresql_config['user'], @postgresql_config['pass'], name, service)

    host, port =  %w{host port}.map { |opt| @postgresql_config[opt] }
    path = File.join(backup_path, "#{name}.dump")

    user =  default_user[:user]
    passwd = default_user[:password]
    archive_list(path, { :restore_bin => @restore_bin })
    cmd = "#{@restore_bin} -h #{host} -p #{port} -U #{user} -L #{path}.archive_list -d #{name} #{path}"
    o, e, s = exe_cmd(cmd)
    return  s.exitstatus == 0
  rescue => e
    @logger.error("Error during restore #{e}")
    nil
  ensure
    FileUtils.rm_rf("#{path}.archive_list")
  end

  # kill user session & block all user
  def disable_instance(prov_cred, binding_creds)
    @logger.debug("Disable instance #{prov_cred["name"]} request.")
    name = prov_cred["name"]
    db_connection = postgresql_connect(@postgresql_config["host"], @postgresql_config["user"], @postgresql_config["pass"], @postgresql_config["port"], name)
    service = Provisionedservice.get(name)
    block_user_from_db(db_connection, service)
    @connection.query("select pg_terminate_backend(procpid) from pg_stat_activity where datname = '#{name}'")
    true
  rescue => e
    @logger.error("Error during disable_instance #{e}")
    nil
  end

  # Dump db content into given path
  def dump_instance(prov_cred, binding_creds, dump_file_path)
    name = prov_cred["name"]
    @logger.debug("Dump instance #{name} request.")
    host, port =  %w{host port}.map { |opt| @postgresql_config[opt] }
    default_user = Provisionedservice.get(name).bindusers.all(:default_user => true)[0]
    if default_user.nil?
      raise "No default user to dump instance."
    else
      user = default_user[:user]
      passwd = default_user[:password]
      dump_file = File.join(dump_file_path, "#{name}.dump")
      @logger.info("Dump instance #{name} content to #{dump_file}")
      cmd = "#{@dump_bin} -Fc -h #{host} -p #{port} -U #{user} -f #{dump_file} #{name}"
      o, e, s = exe_cmd(cmd)
      return s.exitstatus == 0
    end
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
    bind_all_creds(name, binding_creds_hash)
    host, port =  %w{host port}.map { |opt| @postgresql_config[opt] }
    default_user = Provisionedservice.get(name).bindusers.all(:default_user => true)[0]
    if default_user.nil?
      raise "No default user to import instance"
    else
      user = default_user[:user]
      passwd = default_user[:password]
      import_file = File.join(dump_file_path, "#{name}.dump")
      @logger.info("Import data from #{import_file} to database #{name}")
      archive_list(import_file, { :restore_bin => @restore_bin })
      cmd = "#{@restore_bin} -h #{host} -p #{port} -U #{user} -d #{name} -L #{import_file}.archive_list #{import_file}"
      o, e, s = exe_cmd(cmd)
      return s.exitstatus == 0
    end
  rescue => e
    @logger.error("Error during import_instance #{e}")
    nil
  ensure
    FileUtils.rm_rf("#{import_file}.archive_list")
  end

  def enable_instance(prov_cred, binding_creds_hash)
    @logger.debug("Enable instance #{prov_cred["name"]} request.")
    db_connection = postgresql_connect(@postgresql_config["host"], @postgresql_config["user"], @postgresql_config["pass"], @postgresql_config["port"], prov_cred["name"])
    service = Provisionedservice.get(prov_cred["name"])
    unblock_user_from_db(db_connection, service)
    true
  rescue => e
    @logger.error("Error during enable_instance #{e}")
    nil
  end

  def update_instance(prov_cred, binding_creds_hash)
    @logger.debug("Update instance #{prov_cred["name"]} handles request.")
    prov_cred = gen_credential(prov_cred["name"], prov_cred["user"], prov_cred["password"])
    binding_creds_hash.each_value do |v|
      v["credentials"] = gen_credential(prov_cred["name"], v["credentials"]["username"], v["credentials"]["password"])
    end
    [prov_cred, binding_creds_hash]
  rescue => e
    @logger.error("Error during update_instance #{e}")
    []
  end

  # shell CMD wrapper and logger
  def exe_cmd(cmd, env={}, stdin=nil)
    @logger.debug("Execute shell cmd:[#{cmd}]")
    o, e, s = Open3.capture3(env, cmd, :stdin_data => stdin)
    if s.exitstatus == 0
      @logger.info("Execute cmd:[#{cmd}] succeeded.")
    else
      @logger.error("Execute cmd:[#{cmd}] failed. Stdin:[#{stdin}], stdout: [#{o}], stderr:[#{e}]")
    end
    return [o, e, s]
  end

  def varz_details()
    varz = {}
    # pg version
    varz[:pg_version] = @connection.query('select version()')[0]["version"]
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
      Provisionedservice.all.each do |instance|
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

  def get_db_stat
    sys_dbs = ['template0', 'template1', 'postgres']
    result = []
    db_stats = @connection.query('select datid, datname from pg_stat_database')
    db_stats.each do |d|
      name = d["datname"]
      oid = d["datid"]
      next if sys_dbs.include?(name)
      db = {}
      # db name
      db[:name] = name
      # db max size
      db[:max_size] = @max_db_size
      # db actual size
      sizes = @connection.query("select pg_database_size('#{name}')")
      db[:size] = sizes[0]['pg_database_size'].to_i
      # db active connections
      a_s_ps = @connection.query("select pg_stat_get_db_numbackends(#{oid})")
      db[:active_server_processes] = a_s_ps[0]['pg_stat_get_db_numbackends'].to_i
      result << db
    end
    result
  rescue => e
    @logger.warn("Error during generate varz/db_stat: #{e}")
    []
  end

  def get_status(instance)
    res = 'ok'
    host, port = %w{host port}.map { |opt| @postgresql_config[opt] }
    begin
      if instance.bindusers.empty? || instance.bindusers[0].nil?
        @logger.warn('instance without binding?!')
        res = 'fail'
      else
        conn = PGconn.connect(host, port, nil, nil, instance.name,
          instance.bindusers[0].user, instance.bindusers[0].password)
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

  def node_ready?()
    @connection && connection_exception.nil?
  end

  def connection_exception()
    @connection.query("select current_timestamp")
    return nil
  rescue PGError => e
    @logger.warn("PostgreSQL connection lost: #{e}")
    return e
  end
end
