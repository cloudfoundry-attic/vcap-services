$LOAD_PATH.unshift(File.dirname(__FILE__))

module VCAP
  module Services
    module Postgresql
      module WithWarden
      end
    end
  end
end

module VCAP::Services::Postgresql::WithWarden

  include VCAP::Services::Postgresql::Util
  include VCAP::Services::Base::Warden::NodeUtils

  def self.included(base)
    unless base.is_a? VCAP::Services::Postgresql::Node
      raise "WithWarden should be included in a Node instance"
    end
  end

  def pgProvisionedService
    VCAP::Services::Postgresql::Node::Wardenprovisionedservice
  end

  def service_instances
    pgProvisionedService.all
  end

  def pgBindUser
    VCAP::Services::Postgresql::Node::Wardenbinduser
  end

  def pre_send_announcement_prepare
    @connection_mutex = Mutex.new
    @connections = {}
  end

  def pre_send_announcement_internal(options)
    start_all_instances
    @capacity_lock.synchronize{ @capacity -= pgProvisionedService.all.size }
    pgProvisionedService.all.each do |provisionedservice|
      global_connection(provisionedservice, true)
      migrate_instance provisionedservice
    end
    warden_node_init(options)
  end

  def migrate_instance provisionedservice
    nil
  end

  # initialize or reset the persistent connection slot
  def init_global_connection(instance)
    return unless instance
    @connection_mutex.synchronize do
      ignore_exception { @connections[instance.name][:conn].close }
      @connections[instance.name] = { :time => Time.now.to_i, :conn => false }
    end
  end

  def setup_global_connection(instance)
    return unless instance
    conn = postgresql_connect(
      instance.ip,
      postgresql_config(instance)['user'],
      postgresql_config(instance)['pass'],
      instance.service_port,
      postgresql_config(instance)['database']
    )
    @connection_mutex.synchronize do
      @connections[instance.name] = { :time => Time.now.to_i, :conn => conn }
    end
    conn
  end

  def fetch_global_connection(name)
    fetched_conn = { :time => 0, :conn => nil }
    if name
      return fetched_conn unless @connections
      @connection_mutex.synchronize do
        fetched_conn = @connections[name]
      end
    end
    fetched_conn ||= { :time => 0, :conn => nil }
  end

  def delete_global_connection(name)
    @connection_mutex.synchronize do
      conn_info = @connections[name]
      if conn_info
        conn = conn_info[:conn]
        conn.close if conn
        @connections.delete(name)
      end
    end
  end

  def global_connection(instance, keep_alive=false)
    conn = nil
    return conn unless instance
    name = instance.name
    fetched_conn = fetch_global_connection(name)
    time, conn = %w{time, conn}.map { |ele| fetched_conn[ele.to_sym] }
    if keep_alive && (conn != false && (conn.nil? || connection_exception(conn)))
      return nil unless instance.ip
      conn = postgresql_connect(
        instance.ip,
        postgresql_config(instance)['user'],
        postgresql_config(instance)['pass'],
        instance.service_port,
        postgresql_config(instance)['database']
      )
      @connection_mutex.synchronize do
        @connections[name] = { :time => Time.now.to_i, :conn => conn }
      end
    end
    if conn === false
      # will delete the connection slot that is out-of-date
      delete_global_connection(name) if keep_alive && time && (Time.now.to_i - time) > 300
      conn = nil
    end
    conn
  end

  def management_connection(instance, super_user=true, conn_opts={})
    user = super_user ? postgresql_config(instance)['user'] : instance.default_username
    password = super_user ? postgresql_config(instance)['pass'] : instance.default_password
    postgresql_connect(
      instance.ip,
      user,
      password,
      instance.service_port,
      instance.name,
      conn_opts
    )
  end

  def node_ready?
    # check warden server?
    true
  end

  #keep connection alive, and check db liveness
  def postgresql_keep_alive
    acquired = @keep_alive_lock.try_lock
    return unless acquired
    # maintain the global connections
    instances = pgProvisionedService.all.map { |inst| inst }
    temp_lock = Mutex.new
    threads = []
    idx = 0
    10.times do |i|
      threads << Thread.new(i) do |tid|
        loop do
          inst = nil
          temp_lock.synchronize do
            Thread.exit if idx >= instances.size
            inst = instances[idx]
            idx += 1
          end
          global_connection(inst, true) if inst
        end
      end
    end
    threads.each do |t|
      t.join
    end
  rescue => e
    @logger.error("Fail to run postgresql_keep_alive for #{fmt_error(e)}")
  ensure
    @keep_alive_lock.unlock if acquired
  end

  def get_db_stat
    dbs = []
    pgProvisionedService.all.each do |instance|
      conn = global_connection(instance)
      if conn
        res = get_db_stat_by_connection(conn, @max_db_size, @sys_dbs)
        dbs += res
      else
        @logger.warn("No connection to #{instance.name} to get db stat")
      end
    end
    dbs
  end

  def get_db_list
    db_list = []
    pgProvisionedService.all.each do |instance|
      conn = global_connection(instance)
      res = get_db_list_by_connection(conn)
      db_list += res
    end
    db_list
  end

  def db_size(instance)
    size = 0
    conn = global_connection(instance)
    return nil unless conn
    res = conn.query("select pg_tablespace_size('pg_default') + pg_tablespace_size('pg_global') as sum_size")
    res.each do |x|
      size = x["sum_size"].to_i
    end
    size
  end

  def dbs_size(dbs=[])
    dbs = [] if dbs.nil?
    result = {}
    dbs.each do |db|
      db_size(db)
      result[name] = size.to_i
    end
    result
  end

  def postgresql_config(instance)
    return unless instance
    pc = @postgresql_configs[instance.version].dup
    pc['name'] = instance.name
    pc['host'] = instance.ip
    pc['port'] = instance.service_port
    pc
  end

  def kill_long_queries
    acquired = @kill_long_queries_lock.try_lock
    return unless acquired
    pgProvisionedService.all.each do |service|
      conn = global_connection(service)
      @long_queries_killed += kill_long_queries_internal(conn, postgresql_config(service)['user'], @max_long_query) if conn
    end
  rescue => e
    @logger.warn("PostgreSQL Node exception: " + fmt_error(e))
  ensure
    @kill_long_queries_lock.unlock if acquired
  end

  def kill_long_transaction
    acquired = @kill_long_transaction_lock.try_lock
    return unless acquired
    pgProvisionedService.all.each do |service|
      conn = global_connection(service)
      @long_tx_killed += kill_long_transaction_internal(conn, postgresql_config(service)['user'], @max_long_tx) if conn
    end
  rescue => e
    @logger.warn("PostgreSQL Node exception: " + fmt_error(e))
  ensure
    @kill_long_transaction_lock.unlock if acquired
  end

  def shutdown
    super
    @logger.info("Shutting down instances..")
    stop_all_instances
  end

  def get_inst_port(instance)
    instance.port
  end

  def free_inst_port(port)
    free_port(port)
  end

  def set_inst_port(instance, credential)
    @logger.debug("Will reuse the port #{credential['port']}") if credential
    instance.port = new_port((credential['port'] if credential))
  end
end
