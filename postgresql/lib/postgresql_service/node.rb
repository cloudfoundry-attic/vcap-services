# Copyright (c) 2009-2011 VMware, Inc.
require "erb"
require "fileutils"
require "logger"
require "pp"
require "uuidtools"
require "pg"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..', 'base', 'lib')
require 'base/node'
require 'base/service_error'
require "datamapper_l"

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
require "postgresql_service/storage_quota"
require "postgresql_service/postgresql_error"

class VCAP::Services::Postgresql::Node

  KEEP_ALIVE_INTERVAL = 15
  LONG_QUERY_INTERVAL = 1
  STORAGE_QUOTA_INTERVAL = 1

  include VCAP::Services::Postgresql::Util
  include VCAP::Services::Postgresql::Common
  include VCAP::Services::Postgresql

  class Provisionedservice
    include DataMapper::Resource
    property :name,       String,   :key => true
    property :plan,       Enum[:free], :required => true
    property :quota_exceeded,  Boolean, :default => false
    has n, :bindusers
  end

  class Binduser
    include DataMapper::Resource
    property :user,       String,   :key => true
    property :sys_user,    String,    :required => true
    property :password,   String,   :required => true
    property :sys_password,    String,    :required => true
    property :default_user,  Boolean, :default => false
    belongs_to :provisionedservice
  end

  def initialize(options)
    super(options)

    @postgresql_config = options[:postgresql]

    @max_db_size = options[:max_db_size] * 1024 * 1024
    @max_long_query = options[:max_long_query]
    @max_long_tx = options[:max_long_tx]
    @max_db_conns = options[:max_db_conns]

    @connection = postgresql_connect(@postgresql_config["host"],@postgresql_config["user"],@postgresql_config["pass"],@postgresql_config["port"],@postgresql_config["database"])

    EM.add_periodic_timer(KEEP_ALIVE_INTERVAL) {postgresql_keep_alive}
    EM.add_periodic_timer(LONG_QUERY_INTERVAL) {kill_long_queries}
    EM.add_periodic_timer(@max_long_tx/2) {kill_long_transaction}
    EM.add_periodic_timer(STORAGE_QUOTA_INTERVAL) {enforce_storage_quota}

    @base_dir = options[:base_dir]
    FileUtils.mkdir_p(@base_dir) if @base_dir

    DataMapper.setup(:default, options[:local_db])
    DataMapper::auto_upgrade!

    @available_storage = options[:available_storage] * 1024 * 1024
    @node_capacity = @available_storage

    @long_queries_killed = 0
    @long_tx_killed = 0
    @provision_served = 0
    @binding_served = 0

    @mutex_available_storage = Mutex.new
  end

  def pre_send_announcement
    Provisionedservice.all.each do |provisionedservice|
      @available_storage -= storage_for_service(provisionedservice)
    end
    check_db_consistency()
  end

  def get_available_storage
    @mutex_available_storage.synchronize do
      return @available_storage
    end
  end

  def announcement
    a = {
      :available_storage => get_available_storage
    }
    a
  end

  def all_instances_list
    Provisionedservice.all.map{|s| s.name}
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
    @connection.query('select datname,datacl from pg_database').each{|message|
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

  def storage_for_service(provisionedservice)
    case provisionedservice.plan
    when :free then @max_db_size
    else
      raise PostgresqlError.new(PostgresqlError::POSTGRESQL_INVALID_PLAN, provisionedservice.plan)
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

  def kill_long_queries
    process_list = @connection.query("select * from pg_stat_activity")
    process_list.each do |proc|
      if (proc["query_start"] != nil and Time.now.to_i - Time::parse(proc["query_start"]).to_i >= @max_long_query) and (proc["current_query"] != "<IDLE>") and (proc["usename"] != @postgresql_config["user"]) then
        @connection.query("select pg_terminate_backend(#{proc['procpid']})")
        @logger.info("Killed long query: user:#{proc['usename']} db:#{proc['datname']} time:#{Time.now.to_i - Time::parse(proc['query_start']).to_i} info:#{proc['current_query']}")
        @long_queries_killed += 1
      end
    end
  rescue PGError => e
    @logger.warn("PostgreSQL error: #{e}")
  end

  def kill_long_transaction
    process_list = @connection.query("select * from pg_stat_activity")
    process_list.each do |proc|
      if (proc["xact_start"] != nil and Time.now.to_i - Time::parse(proc["xact_start"]).to_i >= @max_long_tx) and (proc["usename"] != @postgresql_config["user"]) then
        @connection.query("select pg_terminate_backend(#{proc['procpid']})")
        @logger.info("Killed long transaction: user:#{proc['usename']} db:#{proc['datname']} active_time:#{Time.now.to_i - Time::parse(proc['xact_start']).to_i}")
        @long_tx_killed += 1
      end
    end
  rescue PGError => e
    @logger.warn("PostgreSQL error: #{e}")
  end

  def provision(plan, credential=nil)
    provisionedservice = Provisionedservice.new
    provisionedservice.plan = plan
    storage = storage_for_service(provisionedservice)

    begin
      @mutex_available_storage.synchronize do
        @available_storage -= storage
      end

      binduser = Binduser.new
      if credential
        name, user, password = %w(name user password).map{|key| credential[key]}
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
      storage = storage_for_service(provisionedservice)
      @mutex_available_storage.synchronize do
        @available_storage += storage
      end
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
    storage = storage_for_service(provisionedservice)

    provisionedservice.bindusers.all.each do |binduser|
      if not binduser.destroy
        @logger.error("Could not delete entry: #{binduser.errors.inspect}")
      end
    end
    if not provisionedservice.destroy
      @logger.error("Could not delete entry: #{provisionedservice.errors.inspect}")
    else
      # restore quota only if provisionedservice is deleted from local db
      @mutex_available_storage.synchronize do
        @available_storage += storage
      end
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
    name, user, bind_opts = %w(name user bind_opts).map{|k| credential[k]}
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
      @logger.debug("Maximum connections: #{@max_db_conns}")
      @connection.query("CREATE DATABASE #{name} WITH CONNECTION LIMIT = #{@max_db_conns}")
      @connection.query("REVOKE ALL ON DATABASE #{name} FROM PUBLIC")
      if not create_database_user(name, bindusers[0], false) then
        raise PostgresqlError.new(PostgresqlError::POSTGRESQL_LOCAL_DB_ERROR)
      end
      storage = storage_for_service(provisionedservice)
      raise PostgresqlError.new(PostgresqlError::POSTGRESQL_DISK_FULL) if get_available_storage < storage
      @logger.info("Done creating #{provisionedservice.inspect}. Took #{Time.now - start}.")
      true
    rescue PGError => e
      @logger.error("Could not create database: #{e}")
      false
    end
  end

  def create_database_user(name, binduser, quota_exceeded)
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
        @connection.query("CREATE ROLE #{user} LOGIN PASSWORD '#{password}'")
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
      @logger.info("Deleting database: #{name}")
      begin
        @connection.query("select pg_terminate_backend(procpid) from pg_stat_activity where datname = '#{name}'")
      rescue PGError => e
        @logger.warn("Could not kill database session: #{e}")
      end
      default_binduser = bindusers.all(:default_user => true)[0]
      @connection.query("DROP DATABASE #{name}")
      @connection.query("DROP ROLE IF EXISTS #{default_binduser.user}") if default_binduser
      @connection.query("DROP ROLE IF EXISTS #{default_binduser.sys_user}") if default_binduser
      true
    rescue PGError => e
      @logger.error("Could not delete database: #{e}")
      false
    end
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
    response = {
      "name" => name,
      "host" => @local_ip,
      "hostname" => @local_ip,
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

  def varz_details()
    varz = {}
    # pg version
    varz[:pg_version] = @connection.query('select version()')[0]["version"]
    # db stat
    varz[:db_stat] = get_db_stat
    # node capacity
    # (no need to synchronize @available_storage here since varz is not in critical path)
    varz[:node_storage_capacity] = @node_capacity
    varz[:node_storage_used] = @node_capacity - @available_storage
    # how many long queries and long txs are killed.
    varz[:long_queries_killed] = @long_queries_killed
    varz[:long_transactions_killed] = @long_tx_killed
    # how many provision/binding operations since startup.
    varz[:provision_served] = @provision_served
    varz[:binding_served] = @binding_served
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

  def healthz_details()
    healthz = {:self => "ok"}
    if connection_exception
      @logger.warn("PostgreSQL connection lost, healthz fail.")
      healthz[:self] = "fail"
      return healthz
    end
    begin
      Provisionedservice.all.each do |instance|
        healthz[instance.name.to_sym] = get_instance_healthz(instance)
      end
    rescue => e
      @logger.error("Error get instance list: #{e}")
      healthz[:self] = "fail"
    end
    healthz
  end

  def get_instance_healthz(instance)
    res = "ok"
    host, port = %w{host port}.map { |opt| @postgresql_config[opt] }
    begin
      conn = PGconn.connect(host, port, nil, nil, instance.name,
        instance.bindusers[0].user, instance.bindusers[0].password)
      conn.query('select current_timestamp')
    rescue => e
      @logger.warn("Error get current timestamp: #{e}")
      res = "fail"
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
