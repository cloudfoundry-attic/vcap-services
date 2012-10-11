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

  def self.included(base)
    unless base.is_a? VCAP::Services::Postgresql::Node
      raise "WithWarden should be included in a Node instance"
    end
  end

  def pgProvisionedService
    VCAP::Services::Postgresql::Node::Wardenprovisionedservice
  end

  def pgBindUser
    VCAP::Services::Postgresql::Node::Wardenbinduser
  end

  def pre_send_announcement_prepare
    @connection_mutex = Mutex.new
    @connections = {}
  end

  def pre_send_announcement_internal
    start_instances(pgProvisionedService.all)
    pgProvisionedService.all.each do |provisionedservice|
      global_connection(provisionedservice, true)
      migrate_instance provisionedservice
    end
  end

  def migrate_instance provisionedservice
    nil
  end

  def init_global_connection(instance)
    return unless instance
    @connection_mutex.synchronize do
      # close first if possible
      if @connections[instance.name] && @connections[instance.name][:conn]
        @connections[instance.name][:conn]
      end
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
          "postgres",
          true
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

  def global_connection(instance=nil, keep_alive=false)
    conn = nil
    if instance
      if instance.is_a?String
        name = instance
      else
        name = instance.name
      end
      fetched_conn = fetch_global_connection(name)
      time = fetched_conn[:time]
      conn = fetched_conn[:conn]
      if keep_alive && (conn != false && (conn.nil? || connection_exception(conn)))
        instance = pgProvisionedService.get(name) if instance.is_a?String
        return nil unless instance.ip
        conn = postgresql_connect(
          instance.ip,
          postgresql_config(instance)['user'],
          postgresql_config(instance)['pass'],
          instance.service_port,
          "postgres",
          true
        )
        @connection_mutex.synchronize do
          @connections[name] = { :time => Time.now.to_i, :conn => conn }
        end
      end
      if conn === false
        delete_global_connection(name) if keep_alive && time && (Time.now.to_i - time) > 300
        conn = nil
      end
    end
    conn
  end

  def management_connection(instance=nil, super_user=true)
    conn = nil
    if instance.is_a?String
      instance = pgProvisionedService.get(instance)
    end
    if instance
      if super_user
        # use the super user defined in the configuration file
        conn = postgresql_connect(
          instance.ip,
          postgresql_config(instance)['user'],
          postgresql_config(instance)['pass'],
          instance.service_port,
          instance.name,
          true
        )
      else
        # use the default user of the service_instance
        conn = postgresql_connect(
          instance.ip,
          instance.default_user,
          instance.default_password,
          instance.service_port,
          instance.name,
          true
        )
      end
    end
    conn
  end

  def node_ready?
    # check warden server?
    true
  end

  #keep connection alive, and check db liveness
  def postgresql_keep_alive
    # maintain the global connections
    pgProvisionedService.all.each do |instance|
      global_connection(instance, true)
    end
  end

  def get_db_stat
    dbs = []
    pgProvisionedService.all.each do |instance|
      conn = global_connection(instance)
      if conn
        res = get_db_stat_by_connection(conn, @max_db_size)
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

  def dbs_size(dbs=[])
    dbs = [] if dbs.nil?
    result = {}
    dbs.each do |db|
      if db.is_a?pgProvisionedService
        name = db.name
      else
        name= db
      end
      res = global_connection(db).query("select pg_database_size(datname) as sum_size from pg_database where datname = '#{name}'")
      res.each do |x|
        size = x["sum_size"]
        result[name] = size.to_i
      end
    end
    result
  end

  def postgresql_config(instance=nil)
    unless instance && instance.is_a?(pgProvisionedService) && instance.name
      @postgresql_config
    else
      pc = @postgresql_config.dup
      pc['name'] = instance.name
      pc['host'] = instance.ip
      pc['port'] = instance.service_port
      pc
    end
  end

  def kill_long_queries
    pgProvisionedService.all.each do |service|
      conn = global_connection(service)
      @long_queries_killed += kill_long_queries_internal(conn, postgresql_config(service)['user'], @max_long_query) if conn
    end
  rescue => e
    @logger.warn("PostgreSQL Node exception: " + fmt_error(e))
  end

  def kill_long_transaction
    pgProvisionedService.all.each do |service|
      conn = global_connection(service)
      @long_tx_killed += kill_long_transaction_internal(conn, postgresql_config(service)['user'], @max_long_tx) if conn
    end
  rescue => e
    @logger.warn("PostgreSQL Node exception: " + fmt_error(e))
  end

  def setup_timers
    EM.add_periodic_timer(VCAP::Services::Postgresql::Node::KEEP_ALIVE_INTERVAL) {postgresql_keep_alive}
    EM.add_periodic_timer(@max_long_query.to_f / 2) {kill_long_queries} if @max_long_query > 0
    EM.add_periodic_timer(@max_long_tx.to_f / 2) {kill_long_transaction} if @max_long_tx > 0
    EM.add_periodic_timer(VCAP::Services::Postgresql::Node::STORAGE_QUOTA_INTERVAL) {enforce_storage_quota}
  end

  def shutdown
    super
    @logger.info("Shutting down instances..")
    stop_instances(pgProvisionedService.all)
  end

  def get_inst_port(instance=nil)
    (instance.port if instance) || @postgresql_config['port']
  end

  def free_inst_port(port)
    free_port(port)
  end

  def set_inst_port(instance, credential)
    @logger.debug("Will reuse the port #{credential['port']}") if credential
    instance.port = new_port((credential['port'] if credential))
  end

  def is_service_started(instance)
    postgresql_quickcheck(
      instance.ip,
      postgresql_config(instance)['user'],
      postgresql_config(instance)['pass'],
      instance.service_port,
      "postgres"
    )
  end
end
