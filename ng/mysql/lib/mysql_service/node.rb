# Copyright (c) 2009-2011 VMware, Inc.
require "erb"
require "fileutils"
require "logger"
require "pp"

require "uuidtools"
require "mysql2"
require "open3"
require "thread"


module VCAP
  module Services
    module Mysql
      class Node < VCAP::Services::Base::Node
        class ProvisionedService
        end
        class WardenProvisionedService < VCAP::Services::Base::Warden::Service
        end
      end
    end
  end
end

require "mysql_service/common"
require "mysql_service/mysql2_timeout"
require "mysql_service/util"
require "mysql_service/storage_quota"
require "mysql_service/mysql_error"
require "mysql_service/transaction_killer"

class VCAP::Services::Mysql::Node

  KEEP_ALIVE_INTERVAL = 15
  STORAGE_QUOTA_INTERVAL = 1

  include VCAP::Services::Mysql::Util
  include VCAP::Services::Mysql::Common
  include VCAP::Services::Mysql

  def initialize(options)
    super(options)
    @use_warden = options[:use_warden]
    @use_warden = false unless @use_warden === true
    if @use_warden
      @logger.debug('using warden')
      require "mysql_service/with_warden"
      self.class.send(:include, VCAP::Services::Mysql::WithWarden)
      self.class.send(:include, VCAP::Services::Base::Utils)
      self.class.send(:include, VCAP::Services::Base::Warden::NodeUtils)
    else
      @logger.debug('not using warden')
      require "mysql_service/without_warden"
      self.class.send(:include, VCAP::Services::Mysql::WithoutWarden)
    end

    init_internal(options)

    @mysql_configs = options[:mysql]
    @connection_pool_size = options[:connection_pool_size]

    @max_db_size = options[:max_db_size] * 1024 * 1024
    @max_long_query = options[:max_long_query]
    @max_long_tx = options[:max_long_tx]
    @kill_long_tx = options[:kill_long_tx]
    @max_user_conns = options[:max_user_conns] || 0
    @gzip_bin = options[:gzip_bin]
    @delete_user_lock = Mutex.new
    @base_dir = options[:base_dir]
    @local_db = options[:local_db]

    @long_queries_killed = 0
    @long_tx_killed = 0
    @long_tx_count = 0
    @long_tx_ids = {}
    @statistics_lock = Mutex.new
    @provision_served = 0
    @binding_served = 0

    #locks
    @keep_alive_lock = Mutex.new
    @kill_long_queries_lock = Mutex.new
    @kill_long_transaction_lock = Mutex.new
    @enforce_quota_lock = Mutex.new

    @connection_wait_timeout = options[:connection_wait_timeout]
    Mysql2::Client.default_timeout = @connection_wait_timeout
    Mysql2::Client.logger = @logger
    @supported_versions = options[:supported_versions]
    mysqlProvisionedService.init(options)
    @transaction_killer = VCAP::Services::Mysql::TransactionKiller.build(options[:mysql_provider])
  end

  def service_instances
    mysqlProvisionedService.all
  end

  def pre_send_announcement
    FileUtils.mkdir_p(@base_dir) if @base_dir

    DataMapper.setup(:default, @local_db)
    DataMapper::auto_upgrade!

    pre_send_announcement_internal(@options)

    EM.add_periodic_timer(STORAGE_QUOTA_INTERVAL) { EM.defer { enforce_storage_quota } }

    keep_alive_interval = KEEP_ALIVE_INTERVAL
    keep_alive_interval = [keep_alive_interval, @connection_wait_timeout.to_f/2].min if @connection_wait_timeout
    EM.add_periodic_timer(keep_alive_interval) { EM.defer { mysql_keep_alive } }
    EM.add_periodic_timer(@max_long_query.to_f/2) { EM.defer { kill_long_queries } } if @max_long_query > 0
    if @max_long_tx > 0
      EM.add_periodic_timer(@max_long_tx.to_f/2) { EM.defer { kill_long_transaction } }
    else
      @logger.info("long transaction killer is disabled.")
    end

    @qps_last_updated = 0
    @queries_served = 0
    # initialize qps counter
    get_qps

    check_db_consistency
  end

  def self.mysqlProvisionedServiceClass(use_warden)
    if use_warden
      VCAP::Services::Mysql::Node::WardenProvisionedService
    else
      VCAP::Services::Mysql::Node::ProvisionedService
    end
  end

  def all_instances_list
    mysqlProvisionedService.all.map { |s| s.name }
  end

  def all_bindings_list
    res = []
    all_ins_users = mysqlProvisionedService.all.map { |s| s.user }
    each_connection_with_port do |connection, port|
      # we can't query plaintext password from mysql since it's encrypted.
      connection.query('select DISTINCT user.user,db from user, db where user.user = db.user and length(user.user) > 0').each do |entry|
        # Filter out the instances handles
        res << gen_credential(entry["db"], entry["user"], "fake-password", port) unless all_ins_users.include?(entry["user"])
      end
    end
    res
  rescue Mysql2::Error => e
    @logger.error("MySQL connection failed: [#{e.errno}] #{e.error}")
    []
  end

  def announcement
    @capacity_lock.synchronize do
      {:available_capacity => @capacity,
        :max_capacity => @max_capacity,
        :capacity_unit => capacity_unit}
    end
  end

  def check_db_consistency()
    db_list = []
    missing_accounts =[]
    each_connection do |connection|
      connection.query('select db, user from db').each(:as => :array) { |row| db_list.push(row) }
    end
    mysqlProvisionedService.all.each do |service|
      account = service.name, service.user
      missing_accounts << account unless db_list.include?(account)
    end
    missing_accounts.each do |account|
      db, user = account
      @logger.warn("Node database inconsistent!!! db:user <#{db}:#{user}> not in mysql.")
    end
    missing_accounts
  rescue Mysql2::Error => e
    @logger.error("MySQL connection failed: [#{e.errno}] #{e.error}")
    nil
  end

  def mysql_connect(mysql_config, exit_on_fail = true)
    host, user, password, port, socket = %w{host user pass port socket}.map { |opt| mysql_config[opt] }

    5.times do
      begin
        return ConnectionPool.new(:host => host, :username => user, :password => password, :database => "mysql", :port => port.to_i, :socket => socket, :logger => @logger, :pool => @connection_pool_size["min"], :pool_min => @connection_pool_size["min"], :pool_max => @connection_pool_size["max"])
      rescue Mysql2::Error => e
        @logger.warn("MySQL connection attempt failed: [#{e.errno}] #{e.error}")
        sleep(1)
      end
    end

    @logger.fatal("MySQL connection to #{host} unrecoverable")
    if exit_on_fail
      shutdown
      exit
    end
  end

  def node_ready?()
    mysqlProvisionedService.all.each do |instance|
      conn_pool = fetch_pool(instance.name)
      return false unless conn_pool && conn_pool.connected?
    end
    true
  end

  #keep connection alive, and check db liveness
  def mysql_keep_alive
    acquired = @keep_alive_lock.try_lock
    return unless acquired
    5.times do
      begin
        each_pool { |conn_pool| conn_pool.keep_alive }
        return
      rescue Mysql2::Error => e
        @logger.error("MySQL connection attempt failed: [#{e.errno}] #{e.error}")
        sleep(5)
      end
    end

    unless @use_warden
      @logger.fatal("MySQL connection unrecoverable")
      shutdown
      exit
    end
  ensure
    @keep_alive_lock.unlock if acquired
  end

  def kill_long_queries
    acquired = @kill_long_queries_lock.try_lock
    return unless acquired
    each_connection do |connection|
      process_list = connection.query("show processlist")
      process_list.each do |proc|
        thread_id, user, db, command, time, info, state = %w(Id User db Command Time Info State).map { |o| proc[o] }
        if (time.to_i >= @max_long_query) and (command == 'Query') and (user != 'root') then
          connection.query("KILL QUERY #{thread_id}")
          @logger.warn("Killed long query: user:#{user} db:#{db} time:#{time} state: #{state} info:#{info}")
          @long_queries_killed += 1
        end
      end
    end
  rescue Mysql2::Error => e
    @logger.error("MySQL error: [#{e.errno}] #{e.error}")
  ensure
    @kill_long_queries_lock.unlock if acquired
  end

  def kill_long_transaction
    acquired = @kill_long_transaction_lock.try_lock
    return unless acquired

    query_str = <<-QUERY
      SELECT * from (
        SELECT trx_started, id, user, db, trx_query, TIME_TO_SEC(TIMEDIFF(NOW() , trx_started )) as active_time
        FROM information_schema.INNODB_TRX t inner join information_schema.PROCESSLIST p
        ON t.trx_mysql_thread_id = p.ID
        WHERE trx_state='RUNNING' and user!='root'
      ) as inner_table
      WHERE inner_table.active_time > #{@max_long_tx}
    QUERY

    each_connection_with_key do |connection, key|
      result = connection.query(query_str)
      current_long_tx_ids = []
      @long_tx_ids[key] = [] if @long_tx_ids[key].nil?
      result.each do |trx|
        trx_started, id, user, db, trx_query, active_time = %w(trx_started id user db trx_query active_time).map { |o| trx[o] }
        if @kill_long_tx
          @transaction_killer.kill(id, connection)
          @logger.warn("Kill long transaction: user:#{user} db:#{db} thread:#{id} trx_query:#{trx_query} active_time:#{active_time}")
          @long_tx_killed += 1
        else
          @logger.warn("Log but not kill long transaction: user:#{user} db:#{db} thread:#{id} trx_query:#{trx_query} active_time:#{active_time}")
          current_long_tx_ids << id
          unless @long_tx_ids[key].include?(id)
            @long_tx_count += 1
          end
        end
      end
      @long_tx_ids[key] = current_long_tx_ids
    end
  rescue => e
    @logger.error("Error during kill long transaction: #{e}.")
  ensure
    @kill_long_transaction_lock.unlock if acquired
  end

  def provision(plan, credential=nil, version=nil)
    raise MysqlError.new(MysqlError::MYSQL_INVALID_PLAN, plan) unless plan == @plan
    raise ServiceError.new(ServiceError::UNSUPPORTED_VERSION, version) unless @supported_versions.include?(version)

    provisioned_service = nil
    begin
      if credential
        name, user, password = %w(name user password).map { |key| credential[key] }
        provisioned_service = mysqlProvisionedService.create(new_port(credential["port"]), name, user, password, version)
      else
        # mysql database name should start with alphabet character
        name = 'd' + UUIDTools::UUID.random_create.to_s.gsub(/-/, '')
        user = 'u' + generate_credential
        password = 'p' + generate_credential
        provisioned_service = mysqlProvisionedService.create(new_port, name, user, password, version)
      end
      provisioned_service.run do |instance|
        setup_pool(instance)
        raise "Could not create database" unless create_database(instance)
      end
      response = gen_credential(provisioned_service.name, provisioned_service.user, provisioned_service.password, get_port(provisioned_service))
      @statistics_lock.synchronize do
        @provision_served += 1
      end
      return response
    rescue => e
      handle_provision_exception(provisioned_service)
      raise e
    end
  end

  def unprovision(name, credentials)
    return if name.nil?
    @logger.debug("Unprovision database:#{name} and its #{credentials.size} bindings")
    provisioned_service = mysqlProvisionedService.get(name)
    raise MysqlError.new(MysqlError::MYSQL_CONFIG_NOT_FOUND, name) if provisioned_service.nil?
    # Delete all bindings, ignore not_found error since we are unprovision
    begin
      credentials.each { |credential| unbind(credential) } if credentials
    rescue => e
      # ignore error, only log it
      @logger.warn("Error found in unbind operation:#{e}")
    end
    delete_database(provisioned_service)

    help_unprovision(provisioned_service)
    @logger.debug("Successfully fulfilled unprovision request: #{name}")
    true
  end

  def bind(name, bind_opts, credential=nil)
    @logger.debug("Bind service for db:#{name}, bind_opts = #{bind_opts}")
    binding = nil
    begin
      service = mysqlProvisionedService.get(name)
      raise MysqlError.new(MysqlError::MYSQL_CONFIG_NOT_FOUND, name) unless service
      # create new credential for binding
      binding = Hash.new
      if credential
        binding[:user] = credential["user"]
        binding[:password] = credential["password"]
      else
        binding[:user] = 'u' + generate_credential
        binding[:password] = 'p' + generate_credential
      end
      binding[:bind_opts] = bind_opts

      begin
        create_database_user(name, binding[:user], binding[:password])
        enforce_instance_storage_quota(service)
      rescue Mysql2::Error => e
        raise "Could not create database user: [#{e.errno}] #{e.error}"
      end

      response = gen_credential(name, binding[:user], binding[:password], get_port(service))
      @logger.debug("Bind response: #{response.inspect}")
      @statistics_lock.synchronize do
        @binding_served += 1
      end
      return response
    rescue => e
      delete_database_user(binding[:user], name) if binding
      raise e
    end
  end

  def unbind(credential)
    return if credential.nil?
    @logger.debug("Unbind service: #{credential.inspect}")
    name, user, bind_opts, passwd = %w(name user bind_opts password).map { |k| credential[k] }

    # Special case for 'ancient' instances that don't have new credentials for each Bind operation.
    # Never delete a user that was created as part of the initial provisioning process.
    @logger.debug("Begin check ancient credentials.")
    mysqlProvisionedService.all(:name => name, :user => user).each { |record| @logger.info("Find unbind credential in local database: #{record.inspect}. Skip delete account."); return true }
    @logger.debug("Ancient credential not found.")

    # validate the existence of credential, in case we delete a normal account because of a malformed credential
    conn_pool = fetch_pool(name)
    if conn_pool.nil?
      @logger.error("fail to fetch connection pool for #{credential.inspect}")
      return
    end
    conn_pool.with_connection do |connection|
      res = connection.query("SELECT * from mysql.user WHERE user='#{user}'")
      raise MysqlError.new(MysqlError::MYSQL_CRED_NOT_FOUND, credential.inspect) if res.count() <= 0
    end
    delete_database_user(user, name)
    conn_pool.with_connection do |connection|
      handle_discarded_routines(name, connection)
    end
    true
  end

  def create_database(provisioned_service)
    name, password, user = [:name, :password, :user].map { |field| provisioned_service.send(field) }
    begin
      start = Time.now
      @logger.debug("Creating: #{provisioned_service.inspect}")
      fetch_pool(name).with_connection do |connection|
        connection.query("CREATE DATABASE #{name}")
      end
      create_database_user(name, user, password)
      @logger.debug("Done creating #{provisioned_service.inspect}. Took #{Time.now - start}.")
      return true
    rescue Mysql2::Error => e
      @logger.warn("Could not create database: [#{e.errno}] #{e.error}")
      return false
    end
  end

  def create_database_user(name, user, password)
    @logger.info("Creating credentials: #{user}/#{password} for database #{name}")
    fetch_pool(name).with_connection do |connection|
      connection.query("GRANT ALL ON #{name}.* to #{user}@'%' IDENTIFIED BY '#{password}' WITH MAX_USER_CONNECTIONS #{@max_user_conns}")
      connection.query("GRANT ALL ON #{name}.* to #{user}@'localhost' IDENTIFIED BY '#{password}' WITH MAX_USER_CONNECTIONS #{@max_user_conns}")
      connection.query("FLUSH PRIVILEGES")
    end
  end

  def delete_database(provisioned_service)
    name, user = [:name, :user].map { |field| provisioned_service.send(field) }
    begin
      delete_database_user(user, name)
      @logger.info("Deleting database: #{name}")
      fetch_pool(name).with_connection do |connection|
        connection.query("DROP DATABASE #{name}")
      end
    rescue Mysql2::Error => e
      @logger.error("Could not delete database: [#{e.errno}] #{e.error}")
    end
  end

  def delete_database_user(user, name)
    @logger.info("Delete user #{user}")
    @delete_user_lock.synchronize do
      ["%", "localhost"].each do |host|
        fetch_pool(name).with_connection do |connection|
          res = connection.query("SELECT user from mysql.user where user='#{user}' and host='#{host}'")
          if res.count == 1
            connection.query("DROP USER #{user}@'#{host}'")
          else
            @logger.warn("Failure to delete non-existent user #{user}@'#{host}'")
          end
        end
      end
      kill_user_session(user, name)
    end
  rescue Mysql2::Error => e
    @logger.error("Could not delete user '#{user}': [#{e.errno}] #{e.error}")
  end

  def kill_user_session(user, name)
    @logger.info("Kill sessions of user: #{user}")
    begin
      fetch_pool(name).with_connection do |connection|
        process_list = connection.query("show processlist")
        process_list.each do |proc|
          thread_id, user_, db, command, time, info = proc["Id"], proc["User"], proc["db"], proc["Command"], proc["Time"], proc["Info"]
          if user_ == user then
            connection.query("KILL #{thread_id}")
            @logger.info("Kill session: user:#{user} db:#{db}")
          end
        end
      end
    rescue Mysql2::Error => e
      # kill session failed error, only log it.
      @logger.error("Could not kill user session.:[#{e.errno}] #{e.error}")
    end
  end

  # restore a given instance using backup file.
  def restore(name, backup_path)
    @logger.debug("Restore db #{name} using backup at #{backup_path}")
    service = mysqlProvisionedService.get(name)
    raise MysqlError.new(MysqlError::MYSQL_CONFIG_NOT_FOUND, name) unless service

    fetch_pool(name).with_connection do |connection|
      # revoke write and lock privileges to prevent race with drop database.
      connection.query("UPDATE db SET insert_priv='N', create_priv='N',
                         update_priv='N', lock_tables_priv='N' WHERE Db='#{name}'")
      connection.query("FLUSH PRIVILEGES")
      kill_database_session(connection, name)
      # mysql can't delete tables that not in dump file.
      # recreate the database to prevent leave unclean tables after restore.
      connection.query("DROP DATABASE #{name}")
      connection.query("CREATE DATABASE #{name}")
      # restore privileges.
      connection.query("UPDATE db SET insert_priv='Y', create_priv='Y',
                         update_priv='Y', lock_tables_priv='Y' WHERE Db='#{name}'")
      connection.query("FLUSH PRIVILEGES")
    end
    host, user, pass, port, socket, mysql_bin = instance_configs(service)
    path = File.join(backup_path, "#{name}.sql.gz")
    cmd = "#{@gzip_bin} -dc #{path}|" +
      "#{mysql_bin} -h #{host} --port='#{port}' --user='#{user}' --password='#{pass}'"
    cmd += " -S #{socket}" unless socket.nil?
    cmd += " #{name}"
    o, e, s = exe_cmd(cmd)
    if s.exitstatus == 0
      # delete the procedures and functions: security_type is definer while the definer doesn't exist
      fetch_pool(name).with_connection do |connection|
        handle_discarded_routines(name, connection)
      end
      return true
    else
      return nil
    end
  rescue => e
    @logger.error("Error during restore #{e}")
    nil
  end

  # Disable all credentials and kill user sessions
  def disable_instance(prov_cred, binding_creds)
    @logger.debug("Disable instance #{prov_cred["name"]} request.")
    binding_creds << prov_cred
    binding_creds.each do |cred|
      unbind(cred)
    end
    true
  rescue => e
    @logger.warn(e)
    nil
  end

  # Dump db content into given path
  def dump_instance(prov_cred, binding_creds, dump_file_path)
    @logger.debug("Dump instance #{prov_cred["name"]} request.")
    name = prov_cred["name"]
    service = mysqlProvisionedService.get(name)
    File.open(File.join(dump_file_path, "#{name}.service"), 'w') { |f| Marshal.dump(service, f) }
    host, user, password, port, socket, _, mysqldump_bin = instance_configs(service)
    dump_file = File.join(dump_file_path, "#{name}.sql")
    @logger.info("Dump instance #{name} content to #{dump_file}")
    cmd = "#{mysqldump_bin} -h #{host} --port='#{port}' --user='#{user}' --password='#{password}' -R --single-transaction #{'-S '+socket if socket} #{name} > #{dump_file}"
    o, e, s = exe_cmd(cmd)
    if s.exitstatus == 0
      return true
    else
      return nil
    end
  rescue => e
    @logger.warn(e)
    nil
  end

  # Provision and import dump files
  # Refer to #dump_instance
  def import_instance(prov_cred, binding_creds_hash, dump_file_path, plan)
    @logger.debug("Import instance #{prov_cred["name"]} request.")
    @logger.info("Provision an instance with plan: #{plan} using data from #{prov_cred.inspect}")

    name = prov_cred["name"]
    dump_service = File.join(dump_file_path, "#{name}.service")
    service = File.open(dump_service, 'r') { |f| Marshal.load(f) }
    raise "Cannot parse dumpfile in #{dump_service}" if service.nil?
    provision(plan, prov_cred, service.version)
    provisioned_service = mysqlProvisionedService.get(name)
    import_file = File.join(dump_file_path, "#{name}.sql")
    host, user, password, port, socket, mysql_bin = instance_configs(provisioned_service)
    @logger.info("Import data from #{import_file} to database #{name}")
    cmd = "#{mysql_bin} --host=#{host} --port='#{port}' --user='#{user}' --password='#{password}' #{'-S '+socket if socket} #{name} < #{import_file}"
    o, e, s = exe_cmd(cmd)
    if s.exitstatus == 0
      # delete the procedures and functions: security_type is definer while the definer doesn't exist
      fetch_pool(name).with_connection do |connection|
        handle_discarded_routines(name, connection)
      end
      return true
    else
      return nil
    end
  rescue => e
    @logger.warn(e)
    nil
  end

  def instance_configs instance
    return unless instance
    config = @mysql_configs[instance.version]
    result = %w{host user pass port socket mysql_bin mysqldump_bin}.map { |opt| config[opt] }
    result[0] = instance.ip if @use_warden

    result
  end

  # Re-bind credentials
  # Refer to #disable_instance
  def enable_instance(prov_cred, binding_creds_hash)
    @logger.debug("Enable instance #{prov_cred["name"]} request.")
    prov_cred = bind(prov_cred["name"], nil, prov_cred)
    binding_creds_hash.each_value do |v|
      cred = v["credentials"]
      binding_opts = v["binding_options"]
      bind(cred["name"], binding_opts, cred)
    end
    true
  rescue => e
    @logger.warn(e)
    nil
  end

  def update_instance(prov_cred, binding_creds_hash)
    @logger.debug("Update instance #{prov_cred["name"]} handles request.")
    name = prov_cred["name"]
    prov_cred = bind(name, nil, prov_cred)
    binding_creds_hash.each_value do |v|
      cred = v["credentials"]
      binding_opts = v["binding_options"]
      v["credentials"] = bind(name, binding_opts, cred)
    end
    [prov_cred, binding_creds_hash]
  rescue => e
    @logger.warn(e)
    []
  end

  # shell CMD wrapper and logger
  def exe_cmd(cmd, stdin=nil)
    @logger.debug("Execute shell cmd:[#{cmd}]")
    o, e, s = Open3.capture3(cmd, :stdin_data => stdin)
    if s.exitstatus == 0
      @logger.info("Execute cmd:[#{cmd}] succeeded.")
    else
      @logger.error("Execute cmd:[#{cmd}] failed. Stdin:[#{stdin}], stdout: [#{o}], stderr:[#{e}]")
    end
    return [o, e, s]
  end

  def varz_details
    varz = super
    # how many queries served since startup
    varz[:queries_since_startup] = get_queries_status
    # queries per second
    varz[:queries_per_second] = get_qps
    # disk usage per instance
    status = get_instance_status
    varz[:database_status] = status
    varz[:max_capacity] = @max_capacity
    varz[:available_capacity] = @capacity
    varz[:used_capacity] = @max_capacity - @capacity
    # how many long queries and long txs are killed.
    varz[:long_queries_killed] = @long_queries_killed
    varz[:long_transactions_killed] = @long_tx_killed
    varz[:long_transactions_count] = @long_tx_count #logged but not killed
    # how many provision/binding operations since startup.
    @statistics_lock.synchronize do
      varz[:provision_served] = @provision_served
      varz[:binding_served] = @binding_served
    end
    # provisioned services status
    varz[:instances] = {}
    begin
      mysqlProvisionedService.all.each do |instance|
        varz[:instances][instance.name.to_sym] = get_status(instance)
      end
    rescue => e
      @logger.error("Error get instance list: #{e}")
    end
    # connection pool information
    varz[:pools] = {}
    each_pool_with_key { |conn_pool, key| varz[:pools][key] = conn_pool.inspect }
    varz
  rescue => e
    @logger.error("Error during generate varz: #{e}")
    {}
  end

  def get_status(instance)
    res = "ok"
    host, root_user, root_pass, port, socket = instance_configs(instance)

    begin
      res = mysql_status(
        :host => host,
        :ins_user => instance.user,
        :ins_pass => instance.password,
        :root_user => root_user,
        :root_pass => root_pass,
        :db => instance.name,
        :port => port.to_i,
        :socket => socket,
      )
    rescue => e
      @logger.warn("Error get status of #{instance.name}: #{e}")
      res = "fail"
    end

    res
  end

  def get_queries_status()
    total = 0
    each_connection do |connection|
      result = connection.query("SHOW STATUS WHERE Variable_name ='QUERIES'")
      total += result.to_a[0]["Value"].to_i if result.count != 0
    end
    total
  end

  def get_qps()
    queries = get_queries_status
    ts = Time.now.to_i
    delta_t = (ts - @qps_last_updated).to_f
    qps = (queries - @queries_served)/delta_t
    @queries_served = queries
    @qps_last_updated = ts
    qps
  rescue Mysql2::Error => e
    @logger.error("MySQL connection failed: [#{e.errno}] #{e.error}")
    0
  end

  def get_instance_status()
    total = []

    each_connection do |connection|
      all_dbs = []
      result = connection.query('show databases')
      result.each { |db| all_dbs << db["Database"] }
      system_dbs = ['mysql', 'information_schema']
      sizes = connection.query(
        'SELECT table_schema "name",
        sum( data_length + index_length ) "size"
        FROM information_schema.TABLES
        GROUP BY table_schema')
      result = []
      db_with_tables = []
      sizes.each do |i|
        db = {}
        name, size = i["name"], i["size"]
        next if system_dbs.include?(name)
        db_with_tables << name
        db[:name] = name
        db[:size] = size.to_i
        db[:max_size] = @max_db_size
        result << db
      end
      # handle empty db without table
      (all_dbs - db_with_tables - system_dbs).each do |db|
        result << {:name => db, :size => 0, :max_size => @max_db_size}
      end
      total += result
    end
    total
  end

  def gen_credential(name, user, passwd, port)
    host = get_host
    response = {
      "name" => name,
      "hostname" => host,
      "host" => host,
      "port" => port,
      "user" => user,
      "username" => user,
      "password" => passwd,
    }
  end

  def get_host
    host = @mysql_configs.values.first['host']
    if ['localhost', '127.0.0.1'].include?(host)
      super
    else
      host
    end
  end

  def each_connection
    each_connection_with_identifier { |conn, identifier| yield conn }
  end

  def each_connection_with_port
    each_connection_with_identifier { |conn, identifier| yield conn, extract_attr(identifier, :port) }
  end

  def each_connection_with_key
    each_connection_with_identifier { |conn, identifier| yield conn, extract_attr(identifier, :key) }
  end

  def each_pool
    each_pool_with_identifier { |conn_pool, identifier| yield conn_pool }
  end

  def each_pool_with_key
    each_pool_with_identifier { |conn_pool, identifier| yield conn_pool, extract_attr(identifier, :key) }
  end

  def each_connection_with_identifier
    each_pool_with_identifier do |conn_pool, identifier|
      begin
        conn_pool.with_connection { |conn| yield conn, identifier }
      rescue => e
        @logger.warn("with_connection failed: #{fmt_error(e)}")
      end
    end
  end
end

class VCAP::Services::Mysql::Node::ProvisionedService
  include DataMapper::Resource
  property :name, String, :key => true
  property :user, String, :required => true
  property :password, String, :required => true
  property :plan, Integer, :required => true
  property :quota_exceeded, Boolean, :default => false
  property :version, String

  class << self
    def create(port, name, user, password, version)
      provisioned_service = new
      provisioned_service.name = name
      provisioned_service.user = user
      provisioned_service.password = password
      provisioned_service.plan = 1
      provisioned_service.version = version
      provisioned_service
    end

    #no-ops methods
    def method_missing(method_name, *args, &block)
      no_ops = [:init]
      super unless no_ops.include?(method_name)
    end
  end

  def run
    yield self if block_given?
    save
  end
end

class VCAP::Services::Mysql::Node::WardenProvisionedService

  include DataMapper::Resource
  include VCAP::Services::Mysql::Util

  property :name, String, :key => true
  property :port, Integer, :unique => true
  property :user, String, :required => true
  property :password, String, :required => true
  property :plan, Integer, :required => true
  property :quota_exceeded, Boolean, :default => false
  property :container, String
  property :ip, String
  property :version, String

  private_class_method :new

  class << self
    def create(port, name, user, password, version)
      raise "Parameter missing" unless port
      provisioned_service = new
      provisioned_service.name = name
      provisioned_service.port = port
      provisioned_service.user = user
      provisioned_service.password = password
      provisioned_service.plan = 1
      provisioned_service.version = version

      provisioned_service.prepare_filesystem(@max_disk)
      FileUtils.mkdir_p(provisioned_service.tmp_dir)
      provisioned_service
    end

    def options
      @@options
    end
  end

  def service_port
    case version
    when "5.5"
      3307
    else
      3306
    end
  end

  def service_conf
    case version
    when "5.5"
      "my55.cnf"
    else
      "my.cnf"
    end
  end

  ["start", "stop", "status"].each do |op|
    define_method "#{op}_script".to_sym do
      passwd = @@options[:mysql][version]["pass"]
      "#{service_script} #{op} /var/vcap/sys/run/mysqld /var/vcap/sys/log/mysql #{common_dir} #{bin_dir} /var/vcap/store/mysql #{version} #{passwd}"
    end
  end

  def tmp_dir
    File.join(base_dir, "tmp")
  end

  def start_options
    options = super
    options[:start_script] = {:script => start_script, :use_spawn => true}
    options[:service_port] = service_port
    update_bind_dirs(options[:bind_dirs], {:src => base_dir}, {:src => base_dir, :dst => "/var/vcap/sys/run/mysqld"})
    update_bind_dirs(options[:bind_dirs], {:src => log_dir}, {:src => log_dir, :dst => "/var/vcap/sys/log/mysql"})
    options[:bind_dirs] << {:src => data_dir, :dst => "/var/vcap/store/mysql"}
    options[:bind_dirs] << {:src => tmp_dir, :dst => "/var/vcap/data/mysql_tmp"}
    options
  end

  def stop_options
    options = super
    options[:stop_script] = {:script => stop_script}
    options
  end

  def status_options
    options = super
    options[:status_script] = {:script => status_script}
    options
  end

  def finish_start?
    # Mysql does this in "setup_pool" function, so just return true here
    true
  end

  def running?
    res = true
    host = self[:ip]
    ins_user = self[:user]
    ins_pass = self[:password]
    db = self[:name]
    mysql_configs = self.class.options[:mysql][self[:version]]
    root_user = mysql_configs["user"]
    root_pass = mysql_configs["pass"]
    port = mysql_configs["port"].to_i
    socket = mysql_configs["socket"]

    begin
      mysql_status(
        :host => host,
        :ins_user => ins_user,
        :ins_pass => ins_pass,
        :root_user => root_user,
        :root_pass => root_pass,
        :db => db,
        :port => port,
        :socket => socket,
      )
    rescue
      res = false
    end

    res
  end
end
