$LOAD_PATH.unshift(File.dirname(__FILE__))

module VCAP
  module Services
    module Postgresql
      module WithoutWarden
      end
    end
  end
end

module VCAP::Services::Postgresql::WithoutWarden

  include VCAP::Services::Postgresql::Util

  def self.included(base)
    unless base == VCAP::Services::Postgresql::Node
      raise "WithoutWarden should be included in a Node instance"
    end
  end

  def pgProvisionedService
    VCAP::Services::Postgresql::Node::Provisionedservice
  end

  def pgBindUser
    VCAP::Services::Postgresql::Node::Binduser
  end

  def pre_send_announcement_prepare
    prepare_global_connections
    @supported_versions.each do |version|
      host, user, pass, port, database =
        %w(host user pass port database).map {|k| @postgresql_configs[version][k]}
      @connections[version] = postgresql_connect(host, user, pass, port, database, :fail_with_nil => false)
    end
  end

  def pre_send_announcement_internal(options)
    pgProvisionedService.all.each do |provisionedservice|
      setup_global_connection provisionedservice
      @capacity -= capacity_unit
    end
  end

  # global connection is a persistent connection to postgresql server
  # each version has one which shard by all instances
  def init_global_connection(instance)
    @connection_mutex.synchronize do
      @connections[instance.name] = instance.version
      @connections[instance.version]
    end
  end

  alias_method :setup_global_connection, :init_global_connection

  # get global persistent connection according to instance's name or version
  def fetch_global_connection(name_or_ver)
    conn = nil
    version = name_or_ver
    @connection_mutex.synchronize do
      version = @supported_versions.include?(name_or_ver) ? name_or_ver : @connections[name_or_ver]
      conn = @connections[version]
    end
    conn
  end

  # only delete slot, not to close
  def delete_global_connection(name_or_ver)
    return if @supported_versions.include?(name_or_ver)
    @connection_mutex.synchronize do
      @connections.delete(name_or_ver)
    end
  end

  # alias of fetch_global_connection
  def global_connection(instance)
    fetch_global_connection(instance.version)
  end

  # create connection for super-user or default user
  def management_connection(instance, super_user=true, conn_opts={})
    conn = nil
    version = instance.version
    name = instance.name
    host, user, pass, port =
      %w(host user pass port).map {|k| @postgresql_configs[version][k]}
    unless super_user
      # use the default user of the service_instance
      default_user = instance.pgbindusers.all(:default_user => true)[0]
      user = default_user.user
      pass = default_user.password
    end
    postgresql_connect(host, user, pass, port, name, conn_opts)
  end

  def node_ready?
    @supported_versions.each do |version|
      conn = fetch_global_connection version
      return false unless (conn && connection_exception(conn).nil?)
    end
    true
  end

  #keep connection alive, and check db liveness
  def postgresql_keep_alive
    acquired = @keep_alive_lock.try_lock
    return unless acquired
    @supported_versions.each do |version|
      conn = fetch_global_connection(version)
      if conn.nil? || connection_exception(conn)
        @logger.warn("PostgreSQL connection for #{version} is lost, trying to keep alive.")
        host, user, pass, port, database =
          %w(host user pass port database).map {|k| @postgresql_configs[version][k]}
        new_conn = postgresql_connect(host, user, pass, port, database, :fail_with_nil => nil, :exception_sleep => 0, :try_num => 1)
        unless new_conn
          @logger.error("Fail not reconnect to postgresql server - #{version}")
          next
        end
        add_discarded_connection(version, conn)
        @connection_mutex.synchronize do
          @connections[version] = new_conn
        end
      end
    end
    close_discarded_connections
  rescue => e
    @logger.error("Fail to run postgresql_keep_alive for #{fmt_error(e)} ")
  ensure
    @keep_alive_lock.unlock if acquired
  end

  def get_db_stat
    @supported_versions.inject([]) do |result, version|
      conn = fetch_global_connection version
      xlog_num =  xlog_file_num(conn.settings['data_directory'])
      result += get_db_stat_by_connection(conn, @max_db_size, @sys_dbs).map! { |db| db[:xlog_num] = xlog_num; db }
    end
  end

  def get_db_list
    @supported_versions.inject([]) do |result, version|
      conn = fetch_global_connection version
      result += get_db_list_by_connection(conn)
    end
  end

  def db_overhead(name)
    avg_overhead = 0
    avg_factor = @max_capacity
    @capacity_lock.synchronize do
      avg_factor = avg_factor - @capacity if @capacity < 0
    end
    res = fetch_global_connection(name).query(
      "select ((sum(pg_database_size(datname)) + avg(pg_tablespace_size('pg_global')))/#{avg_factor})
      as avg_overhead from pg_database where datname in ('#{@sys_dbs.join('\', \'')}');")
    res.each do |x|
      avg_overhead = x['avg_overhead'].to_f.ceil
    end
    avg_overhead
  end

  def db_size(instance)
    sum = 0
    avg_overhead = db_overhead(instance.name)
    sz = global_connection(instance).query("select pg_database_size('#{instance.name}') size")
    sz.each do |x|
      sum += x['size'].to_i + avg_overhead
    end
    sum
  end

  def dbs_size()
    result = {}
    @supported_versions.each do |version|
      avg_overhead = db_overhead(version)
      res = fetch_global_connection(version).query("select datname, sum(pg_database_size(datname)) as sum_size from pg_database group by datname")
      res.each do |x|
        name, size = x["datname"], x["sum_size"]
        result[name] = (size.to_i + avg_overhead) unless @sys_dbs.include?(name)
      end
    end
    result
  end

  def postgresql_config(instance)
    return unless instance
    pc = @postgresql_configs[instance.version].dup
    pc['name'] = instance.name
    pc
  end

  def kill_long_queries
    acquired = @kill_long_queries_lock.try_lock
    return unless acquired
    @supported_versions.each do |version|
      conn = @connections[version]
      super_user = @postgresql_configs[version]['user']
      @long_queries_killed +=  kill_long_queries_internal(conn, super_user, @max_long_query)
    end
  rescue => e
    @logger.warn("PostgreSQL Node exception: " + fmt_error(e))
  ensure
    @kill_long_queries_lock.unlock if acquired
  end

  def kill_long_transaction
    acquired = @kill_long_transaction_lock.try_lock
    return unless acquired
    @supported_versions.each do |version|
      conn = @connections[version]
      super_user = @postgresql_configs[version]['user']
      @long_tx_killed += kill_long_transaction_internal(conn, super_user, @max_long_tx)
    end
  rescue => e
    @logger.warn("PostgreSQL Node exception: " + fmt_error(e))
  ensure
    @kill_long_transaction_lock.unlock if acquired
  end

  def get_inst_port(instance)
    @postgresql_configs[instance.version]['port']
  end

  def free_inst_port(port)
    true
  end

  def set_inst_port(instance, credential)
    true
  end

  def xlog_enforce
    acquired = @enforce_xlog_lock.try_lock
    return unless acquired
    @supported_versions.each do |version|
      conn = @connections[version]
      if xlog_status(conn, conn.settings['data_directory']) != VCAP::Services::Postgresql::Node::XLOG_STATUS_OK
        xlog_enforce_internal(conn, :alert_only => true)
      end
    end
  rescue => e
    @logger.warn("PostgreSQL Node exception: " + fmt_error(e))
  ensure
    @enforce_xlog_lock.unlock if acquired
  end

end
