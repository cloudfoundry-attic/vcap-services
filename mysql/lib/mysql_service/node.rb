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
      end
    end
  end
end

require "mysql_service/common"
require "mysql_service/util"
require "mysql_service/storage_quota"
require "mysql_service/mysql_error"

class VCAP::Services::Mysql::Node

  KEEP_ALIVE_INTERVAL = 15
  STORAGE_QUOTA_INTERVAL = 1

  include VCAP::Services::Mysql::Util
  include VCAP::Services::Mysql::Common
  include VCAP::Services::Mysql

  class ProvisionedService
    include DataMapper::Resource
    property :name,       String,   :key => true
    property :user,       String,   :required => true
    property :password,   String,   :required => true
    # property plan is deprecated. The instances in one node have same plan.
    property :plan,       Integer,  :required => true
    property :quota_exceeded,  Boolean, :default => false
  end

  def initialize(options)
    super(options)

    @mysql_config = options[:mysql]
    @connection_pool_size = options[:connection_pool_size]

    @max_db_size = options[:max_db_size] * 1024 * 1024
    @max_long_query = options[:max_long_query]
    @max_long_tx = options[:max_long_tx]
    @max_user_conns = options[:max_user_conns] || 0
    @mysqldump_bin = options[:mysqldump_bin]
    @gzip_bin = options[:gzip_bin]
    @mysql_bin = options[:mysql_bin]
    @delete_user_lock = Mutex.new
    @base_dir = options[:base_dir]
    @local_db = options[:local_db]

    @long_queries_killed = 0
    @long_tx_killed = 0
    @statistics_lock = Mutex.new
    @provision_served = 0
    @binding_served = 0
  end

  def pre_send_announcement
    @pool = mysql_connect
    EM.add_periodic_timer(KEEP_ALIVE_INTERVAL) {mysql_keep_alive}
    EM.add_periodic_timer(@max_long_query.to_f/2) {kill_long_queries} if @max_long_query > 0
    if (@max_long_tx > 0) and (check_innodb_plugin)
      EM.add_periodic_timer(@max_long_tx.to_f/2) {kill_long_transaction}
    else
      @logger.info("long transaction killer is disabled.")
    end
    EM.add_periodic_timer(STORAGE_QUOTA_INTERVAL) {enforce_storage_quota}

    FileUtils.mkdir_p(@base_dir) if @base_dir

    DataMapper.setup(:default, @local_db)
    DataMapper::auto_upgrade!
    @queries_served = 0
    @qps_last_updated = 0
    # initialize qps counter
    get_qps

    @capacity_lock.synchronize do
      ProvisionedService.all.each do |provisionedservice|
        @capacity -= capacity_unit
      end
    end
    check_db_consistency
  end

  def all_instances_list
    ProvisionedService.all.map{|s| s.name}
  end

  def all_bindings_list
    res = []
    all_ins_users = ProvisionedService.all.map{|s| s.user}
    @pool.with_connection do |connection|
      # we can't query plaintext password from mysql since it's encrypted.
      connection.query('select DISTINCT user.user,db from user, db where user.user = db.user and length(user.user) > 0').each do |entry|
        # Filter out the instances handles
        res << gen_credential(entry["db"], entry["user"], "fake-password") unless all_ins_users.include?(entry["user"])
      end
    end
    res
  rescue Mysql2::Error => e
    @logger.error("MySQL connection failed: [#{e.errno}] #{e.error}")
    []
  end

  def announcement
    @capacity_lock.synchronize do
      { :available_capacity => @capacity,
        :capacity_unit => capacity_unit }
    end
  end

  def check_db_consistency()
    db_list = []
    missing_accounts =[]
    @pool.with_connection do |connection|
      connection.query('select db, user from db').each(:as => :array){|row| db_list.push(row)}
    end
    ProvisionedService.all.each do |service|
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

  # check whether mysql has required innodb plugin installed.
  def check_innodb_plugin()
    @pool.with_connection do |connection|
      res = connection.query("show tables from information_schema like 'INNODB_TRX'")
      return true if res.count > 0
    end
  rescue Mysql2::Error => e
    @logger.error("MySQL connection failed: [#{e.errno}] #{e.error}")
    nil
  end

  def mysql_connect
    host, user, password, port, socket =  %w{host user pass port socket}.map { |opt| @mysql_config[opt] }

    5.times do
      begin
        return ConnectionPool.new(:host => host, :username => user, :password => password, :database => "mysql", :port => port.to_i, :socket => socket, :logger => @logger, :pool => @connection_pool_size)
      rescue Mysql2::Error => e
        @logger.error("MySQL connection attempt failed: [#{e.errno}] #{e.error}")
        sleep(5)
      end
    end

    @logger.fatal("MySQL connection unrecoverable")
    shutdown
    exit
  end

  def node_ready?()
    @pool && @pool.connected?
  end

  #keep connection alive, and check db liveness
  def mysql_keep_alive
    5.times do
      begin
        @pool.keep_alive
        return
      rescue Mysql2::Error => e
        @logger.error("MySQL connection attempt failed: [#{e.errno}] #{e.error}")
        sleep(5)
      end
    end

    @logger.fatal("MySQL connection unrecoverable")
    shutdown
    exit
  end

  def kill_long_queries
    @pool.with_connection do |connection|
      process_list = connection.query("show processlist")
      process_list.each do |proc|
        thread_id, user, db, command, time, info, state = %w(Id User db Command Time Info State).map{|o| proc[o]}
        if (time.to_i >= @max_long_query) and (command == 'Query') and (user != 'root') then
          connection.query("KILL QUERY #{thread_id}")
          @logger.warn("Killed long query: user:#{user} db:#{db} time:#{time} state: #{state} info:#{info}")
          @long_queries_killed += 1
        end
      end
    end
  rescue Mysql2::Error => e
    @logger.error("MySQL error: [#{e.errno}] #{e.error}")
  end

  def kill_long_transaction
    query_str = "SELECT * from ("+
                "  SELECT trx_started, id, user, db, info, TIME_TO_SEC(TIMEDIFF(NOW() , trx_started )) as active_time" +
                "  FROM information_schema.INNODB_TRX t inner join information_schema.PROCESSLIST p " +
                "  ON t.trx_mysql_thread_id = p.ID " +
                "  WHERE trx_state='RUNNING' and user!='root' " +
                ") as inner_table " +
                "WHERE inner_table.active_time > #{@max_long_tx}"
    @pool.with_connection do |connection|
      result = connection.query(query_str)
      result.each do |trx|
        trx_started, id, user, db, info, active_time = %w(trx_started id user db info active_time).map{|o| trx[o]}
        connection.query("KILL QUERY #{id}")
        @logger.warn("Kill long transaction: user:#{user} db:#{db} thread:#{id} info:#{info} active_time:#{active_time}")
        @long_tx_killed += 1
      end
    end
  rescue => e
    @logger.error("Error during kill long transaction: #{e}.")
  end

  def provision(plan, credential=nil)
    raise MysqlError.new(MysqlError::MYSQL_INVALID_PLAN, plan) unless plan == @plan
    provisioned_service = ProvisionedService.new
    provisioned_service.plan = 1
    begin
      if credential
        name, user, password = %w(name user password).map{|key| credential[key]}
        provisioned_service.name = name
        provisioned_service.user = user
        provisioned_service.password = password
      else
        # mysql database name should start with alphabet character
        provisioned_service.name = 'd' + UUIDTools::UUID.random_create.to_s.gsub(/-/, '')
        provisioned_service.user = 'u' + generate_credential
        provisioned_service.password = 'p' + generate_credential
      end
      raise "Could not create database" unless create_database(provisioned_service)

      if not provisioned_service.save
        @logger.error("Could not save entry: #{provisioned_service.errors.inspect}")
        raise MysqlError.new(MysqlError::MYSQL_LOCAL_DB_ERROR)
      end
      response = gen_credential(provisioned_service.name, provisioned_service.user, provisioned_service.password)
      @statistics_lock.synchronize do
        @provision_served += 1
      end
      return response
    rescue => e
      delete_database(provisioned_service)
      raise e
    end
  end

  def unprovision(name, credentials)
    return if name.nil?
    @logger.debug("Unprovision database:#{name} and its #{credentials.size} bindings")
    provisioned_service = ProvisionedService.get(name)
    raise MysqlError.new(MysqlError::MYSQL_CONFIG_NOT_FOUND, name) if provisioned_service.nil?
    # TODO: validate that database files are not lingering
    # Delete all bindings, ignore not_found error since we are unprovision
    begin
      credentials.each{ |credential| unbind(credential)} if credentials
    rescue =>e
      # ignore error, only log it
      @logger.warn("Error found in unbind operation:#{e}")
    end
    delete_database(provisioned_service)
    if not provisioned_service.destroy
      @logger.error("Could not delete service: #{provisioned_service.errors.inspect}")
      raise MysqlError.new(MysqError::MYSQL_LOCAL_DB_ERROR)
    end
    # the order is important, restore quota only when record is deleted from local db.
    @logger.debug("Successfully fulfilled unprovision request: #{name}")
    true
  end

  def bind(name, bind_opts, credential=nil)
    @logger.debug("Bind service for db:#{name}, bind_opts = #{bind_opts}")
    binding = nil
    begin
      service = ProvisionedService.get(name)
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
      rescue Mysql2::Error => e
        raise "Could not create database user: [#{e.errno}] #{e.error}"
      end

      response = gen_credential(name, binding[:user], binding[:password])
      @logger.debug("Bind response: #{response.inspect}")
      @statistics_lock.synchronize do
        @binding_served += 1
      end
      return response
    rescue => e
      delete_database_user(binding[:user]) if binding
      raise e
    end
  end

  def unbind(credential)
    return if credential.nil?
    @logger.debug("Unbind service: #{credential.inspect}")
    name, user, bind_opts,passwd = %w(name user bind_opts password).map{|k| credential[k]}

    # Special case for 'ancient' instances that don't have new credentials for each Bind operation.
    # Never delete a user that was created as part of the initial provisioning process.
    @logger.debug("Begin check ancient credentials.")
    ProvisionedService.all(:name => name, :user => user).each {|record| @logger.info("Find unbind credential in local database: #{record.inspect}. Skip delete account."); return true}
    @logger.debug("Ancient credential not found.")

    # validate the existence of credential, in case we delete a normal account because of a malformed credential
    @pool.with_connection do |connection|
      res = connection.query("SELECT * from mysql.user WHERE user='#{user}'")
      raise MysqlError.new(MysqlError::MYSQL_CRED_NOT_FOUND, credential.inspect) if res.count() <= 0
    end
    delete_database_user(user)
    true
  end

  def create_database(provisioned_service)
    name, password, user = [:name, :password, :user].map { |field| provisioned_service.send(field) }
    begin
      start = Time.now
      @logger.debug("Creating: #{provisioned_service.inspect}")
      @pool.with_connection do |connection|
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
      @pool.with_connection do |connection|
        connection.query("GRANT ALL ON #{name}.* to #{user}@'%' IDENTIFIED BY '#{password}' WITH MAX_USER_CONNECTIONS #{@max_user_conns}")
        connection.query("GRANT ALL ON #{name}.* to #{user}@'localhost' IDENTIFIED BY '#{password}' WITH MAX_USER_CONNECTIONS #{@max_user_conns}")
        connection.query("FLUSH PRIVILEGES")
      end
  end

  def delete_database(provisioned_service)
    name, user = [:name, :user].map { |field| provisioned_service.send(field) }
    begin
      delete_database_user(user)
      @logger.info("Deleting database: #{name}")
      @pool.with_connection do |connection|
        connection.query("DROP DATABASE #{name}")
      end
    rescue Mysql2::Error => e
      @logger.error("Could not delete database: [#{e.errno}] #{e.error}")
    end
  end

  def delete_database_user(user)
    @logger.info("Delete user #{user}")
    @delete_user_lock.synchronize do
      ["%", "localhost"].each do |host|
        @pool.with_connection do |connection|
          res = connection.query("SELECT user from mysql.user where user='#{user}' and host='#{host}'")
          if res.count == 1
            connection.query("DROP USER #{user}@'#{host}'")
          else
            @logger.warn("Failure to delete non-existent user #{user}@'#{host}'")
          end
        end
      end
      kill_user_session(user)
    end
  rescue Mysql2::Error => e
    @logger.error("Could not delete user '#{user}': [#{e.errno}] #{e.error}")
  end

  def kill_user_session(user)
    @logger.info("Kill sessions of user: #{user}")
    begin
      @pool.with_connection do |connection|
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
    service = ProvisionedService.get(name)
    raise MysqlError.new(MysqlError::MYSQL_CONFIG_NOT_FOUND, name) unless service
    @pool.with_connection do |connection|
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
    host, user, pass, port, socket =  %w{host user pass port socket}.map { |opt| @mysql_config[opt] }
    path = File.join(backup_path, "#{name}.sql.gz")
    cmd = "#{@gzip_bin} -dc #{path}|" +
      "#{@mysql_bin} -h #{host} -P #{port} -u #{user} --password=#{pass}"
    cmd += " -S #{socket}" unless socket.nil?
    cmd += " #{name}"
    o, e, s = exe_cmd(cmd)
    if s.exitstatus == 0
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
  rescue  => e
    @logger.warn(e)
    nil
  end

  # Dump db content into given path
  def dump_instance(prov_cred, binding_creds, dump_file_path)
    @logger.debug("Dump instance #{prov_cred["name"]} request.")
    name = prov_cred["name"]
    host, user, password, port, socket =  %w{host user pass port socket}.map { |opt| @mysql_config[opt] }
    dump_file = File.join(dump_file_path, "#{name}.sql")
    @logger.info("Dump instance #{name} content to #{dump_file}")
    cmd = "#{@mysqldump_bin} -h #{host} -u #{user} --password=#{password} --single-transaction #{'-S '+socket if socket} #{name} > #{dump_file}"
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
    provision(plan, prov_cred)
    name = prov_cred["name"]
    import_file = File.join(dump_file_path, "#{name}.sql")
    host, user, password, port, socket =  %w{host user pass port socket}.map { |opt| @mysql_config[opt] }
    @logger.info("Import data from #{import_file} to database #{name}")
    cmd = "#{@mysql_bin} --host=#{host} --user=#{user} --password=#{password} #{'-S '+socket if socket} #{name} < #{import_file}"
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

  # Re-bind credentials
  # Refer to #disable_instance
  def enable_instance(prov_cred, binding_creds_hash)
    @logger.debug("Enable instance #{prov_cred["name"]} request.")
    prov_cred = bind(prov_cred["name"], nil, prov_cred)
    binding_creds_hash.each_value do |v|
      cred = v["credentials"]
      binding_opts = v["binding_options"]
      bind(v["credentials"]["name"], v["binding_options"], v["credentials"])
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

  def varz_details()
    varz = {}
    # how many queries served since startup
    varz[:queries_since_startup] = get_queries_status
    # queries per second
    varz[:queries_per_second] = get_qps
    # disk usage per instance
    status = get_instance_status
    varz[:database_status] = status
    varz[:max_capacity] = @max_capacity
    varz[:available_capacity] = @capacity
    # how many long queries and long txs are killed.
    varz[:long_queries_killed] = @long_queries_killed
    varz[:long_transactions_killed] = @long_tx_killed
    # how many provision/binding operations since startup.
    @statistics_lock.synchronize do
      varz[:provision_served] = @provision_served
      varz[:binding_served] = @binding_served
    end
    # provisioned services status
    varz[:instances] = {}
    begin
      ProvisionedService.all.each do |instance|
        varz[:instances][instance.name.to_sym] = get_status(instance)
      end
    rescue => e
      @logger.error("Error get instance list: #{e}")
    end
    varz
  rescue => e
    @logger.error("Error during generate varz: #{e}")
    {}
  end

  def get_status(instance)
    res = "ok"
    host, port, socket, root_user, root_pass = %w{host port socket user pass}.map { |opt| @mysql_config[opt] }
    begin
      begin
        conn = Mysql2::Client.new(:host => host, :username => instance.user, :password => instance.password, :database =>instance.name, :port => port.to_i, :socket => socket)
      rescue Mysql2::Error => e
        # user had modified instance password, fallback to root account
        conn = Mysql2::Client.new(:host => host, :username => root_user, :password => root_pass, :database =>instance.name, :port => port.to_i, :socket => socket)
        res = "password-modified"
      end
      conn.query("SHOW TABLES")
    rescue => e
      @logger.warn("Error get tables of #{instance.name}: #{e}")
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

  def get_queries_status()
    @pool.with_connection do |connection|
      result = connection.query("SHOW STATUS WHERE Variable_name ='QUERIES'")
      return 0 if result.count == 0
      return result.to_a[0]["Value"].to_i
    end
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
    all_dbs = []
    @pool.with_connection do |connection|
      result = connection.query('show databases')
      result.each {|db| all_dbs << db["Database"]}
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
      (all_dbs - db_with_tables - system_dbs ).each do |db|
        result << {:name => db, :size => 0, :max_size => @max_db_size}
      end
      return result
    end
  end

  def gen_credential(name, user, passwd)
    host = get_host
    response = {
      "name" => name,
      "hostname" => host,
      "host" => host,
      "port" => @mysql_config['port'],
      "user" => user,
      "username" => user,
      "password" => passwd,
    }
  end

  def is_percona_server?()
    @pool.with_connection do |connection|
      res = connection.query("show variables where variable_name like 'version_comment'")
      return res.count > 0 && res.to_a[0]["Value"] =~ /percona/i
    end
  rescue Mysql2::Error => e
    @logger.error("MySQL connection failed: [#{e.errno}] #{e.error}")
    nil
  end
end
