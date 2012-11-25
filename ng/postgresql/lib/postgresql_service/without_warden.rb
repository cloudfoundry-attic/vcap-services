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
    unless base.is_a? VCAP::Services::Postgresql::Node
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
    @connection_mutex = Mutex.new
    @connections = {}

    @supported_versions.each do |version|
      host, user, pass, port, database =
        %w(host user pass port database).map {|k| @postgresql_configs[version][k]}
      @connections[version] = postgresql_connect(host, user, pass, port, database)
    end
  end

  def pre_send_announcement_internal(options)
    pgProvisionedService.all.each do |provisionedservice|
      setup_global_connection provisionedservice
      migrate_instance provisionedservice
      @capacity -= capacity_unit
    end
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
    # Services-r11 and earlier the user could not have temp privilege to
    # create temporary tables/views/sequences. Services-r12 solves this issue.
    manage_temp_privilege(provisionedservice.name)
    # In earlier releases, users should not have create privilege to create schmea in databases.
    manage_create_privilege(provisionedservice.name)
    # Fix the bug: when restoring database, the max connection limit is set to -1
    # https://www.pivotaltracker.com/story/show/34260725
    manage_maxconnlimit(provisionedservice.name)
  end

  def get_expected_children(name)
    # children according to pgProvisionedService
    children = pgProvisionedService.get(name).pgbindusers.all(:default_user => false)
    children = children.map { |child| child.user } + children.map { |child| child.sys_user }
    children
  end

  def get_actual_children(connection, name, parent)
    instance = pgProvisionedService.get(name)
    raise "Can't find instance #{name}" unless instance
    version = instance.version

    # children according to postgres itself
    children = []
    rows = connection.query("SELECT datacl FROM pg_database WHERE datname='#{name}'")
    raise "Can't get datacl" if rows.nil? || rows.num_tuples < 1
    datacl = rows[0]['datacl']
    # a typical pg_database.datacl value:
    # {vcap=CTc/vcap,suf4f57864f519412b82ffd0b75d02dcd1=c/vcap,u2e47852f15544536b2f69c0f72052847=c/vcap,su76f8095858e742d1954544c722b277f8=c/vcap,u02b45d2974644895b1b03a92749250b2=c/vcap,su7950e259bbe946328ba4e3540c141f4b=c/vcap,uaf8982bc76324c6e9a09596fa1e57fc3=c/vcap}
    raise "Datacl is nil/deformed" if datacl.nil? || datacl.length < 2
    nonchildren = [@postgresql_configs[version]["user"], parent.user, parent.sys_user, '']
    datacl[1,datacl.length-1].split(',').each do |aclitem|
      child = aclitem.split('=')[0]
      children << child unless nonchildren.include?(child)
    end
    children
  end

  def get_ruly_children(connection, parent)
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
    ruly_children
  end

  def get_unruly_children(connection, parent, children)
    # children which are not in fact children of the parent. (we don't
    # handle children that somehow have the *wrong* parent, but that
    # won't happen :-)
    children - get_ruly_children(connection, parent)
  end

  def manage_object_ownership(name)
    # figure out which children *should* exist
    expected_children = get_expected_children name
    # optimization: the set of children we need to take action for is
    # a subset of the expected childen, so if there are no expected
    # children we can stop right now
    return if expected_children.empty?
    # the parent role
    instance = pgProvisionedService.get(name)
    parent = instance.pgbindusers.all(:default_user => true)[0]
    # connect as the system user (not the parent or any of the
    # children) to ensure we don't have ACL problems
    connection = management_connection(instance, true)
    raise "Fail to connect to database #{name}" unless connection
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

  def manage_temp_privilege(name)
    instance = pgProvisionedService.get(name)
    return if instance.quota_exceeded
    connection = management_connection(instance, true)
    raise "Fail to connect to database #{name}" unless connection
    parent = pgProvisionedService.get(name).pgbindusers.all(:default_user => true)[0]
    connection.query("GRANT TEMP ON DATABASE #{name} TO #{parent.user}")
    connection.query("GRANT TEMP ON DATABASE #{name} TO #{parent.sys_user}")
    expected_children = get_expected_children name
    return expected_children if expected_children.empty?
    actual_children = get_actual_children connection, name, parent
    children = expected_children & actual_children
    @logger.warn "Ignoring surplus children #{actual_children-children} in #{name} when managing temp privilege" unless (actual_children-children).empty?
    @logger.warn "Ignoring missing children #{expected_children-children} in #{name} when managing temp privilege" unless (expected_children-children).empty?
    return if children.empty?
    # manage_object_ownership will make all unruly children be ruly children
    children.each do |i_c|
      connection.query("GRANT TEMP ON DATABASE #{name} TO #{i_c}")
    end
  rescue => x
    @logger.warn("Exception while managing temp privilege on database #{name}: #{x}")
  ensure
    connection.close if connection
  end

  def manage_create_privilege(name)
    instance = pgProvisionedService.get(name)
    return if instance.quota_exceeded
    connection = management_connection(instance, true)
    raise "Fail to connect to database #{name}" unless connection
    parent = pgProvisionedService.get(name).pgbindusers.all(:default_user => true)[0]
    connection.query("GRANT CREATE ON DATABASE #{name} TO #{parent.user}")
  rescue => x
    @logger.warn("Exception while managing create privilege on database #{name}: #{x}")
  ensure
    connection.close if connection
  end

  def manage_maxconnlimit(name)
    conn = fetch_global_connection name
    conn.query("update pg_database set datconnlimit=#{@max_db_conns} where datname='#{name}' and datconnlimit=-1")
  rescue => x
    @logger.warn("Exception while managing maxconnlimit on database #{name}: #{x}")
  end

  def init_global_connection(instance)
    @connection_mutex.synchronize do
      @connections[instance.name] ||= @connections[instance.version]
    end
    @connections[instance.name]
  end

  def setup_global_connection(instance)
    init_global_connection instance
  end

  def global_connection(instance)
    @connection_mutex.synchronize do
      @connections[instance.name]
    end
  end

  def management_connection(instance, super_user=true)
    conn = nil
    version = instance.version
    db_name = instance.name
    host, user, pass, port =
      %w(host user pass port).map {|k| @postgresql_configs[version][k]}
    if super_user
      conn = postgresql_connect(host, user, pass, port, db_name, :fail_with_nil => true)
    else
      # use the default user of the service_instance
      default_user = instance.pgbindusers.all(:default_user => true)[0]
      conn = postgresql_connect(host, default_user.user, default_user.password, port, db_name, :fail_with_nil => true)
    end
    conn
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
    @supported_versions.each do |version|
      if connection_exception(@connections[version])
        @logger.warn("PostgreSQL connection for #{version} is lost, trying to keep alive.")
        host, user, pass, port, database =
          %w(host user pass port database).map {|k| @postgresql_configs[version][k]}
        @connections[version] = postgresql_connect(host, user, pass, port, database, :fail_with_nil => true)
      end
    end
  end

  def get_db_stat
    @supported_versions.inject([]) do |result, version|
      conn = fetch_global_connection version
      result +=  get_db_stat_by_connection(conn, @max_db_size, @sys_dbs)
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
    res = fetch_global_connection(name).query("select ((sum(pg_database_size(datname)) + avg(pg_tablespace_size('pg_global')))/#{@capacity}) as avg_overhead from pg_database where datname in ('#{@sys_dbs.join('\', \'')}');")
    res.each do |x|
      avg_overhead = x['avg_overhead'].to_f.ceil
    end
    avg_overhead
  end

  def db_size(db)
    sum = 0
    avg_overhead = db_overhead(db.name)
    sz = global_connection(db).query("select pg_database_size('#{db.name}') size")
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
    @supported_versions.each do |version|
      conn = @connections[version]
      super_user = @postgresql_configs[version]['user']
      @long_queries_killed +=  kill_long_queries_internal(conn, super_user, @max_long_query)
    end
  end

  def kill_long_transaction
    @supported_versions.each do |version|
      conn = @connections[version]
      super_user = @postgresql_configs[version]['user']
      @long_tx_killed += kill_long_transaction_internal(conn, super_user, @max_long_tx)
    end
  end

  def setup_timers
    EM.add_periodic_timer(VCAP::Services::Postgresql::Node::KEEP_ALIVE_INTERVAL) {postgresql_keep_alive}
    EM.add_periodic_timer(@max_long_query.to_f / 2) {kill_long_queries} if @max_long_query > 0
    EM.add_periodic_timer(@max_long_tx.to_f / 2) {kill_long_transaction} if @max_long_tx > 0
    EM.add_periodic_timer(VCAP::Services::Postgresql::Node::STORAGE_QUOTA_INTERVAL) {enforce_storage_quota}
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

  def fetch_global_connection(name)
    @connection_mutex.synchronize do
      @connections[name]
    end
  end

  def delete_global_connection(name)
    @connection_mutex.synchronize do
      @connections.delete(name)
    end
  end
end
