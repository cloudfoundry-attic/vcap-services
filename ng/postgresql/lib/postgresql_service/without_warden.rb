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
    @connection = global_connection
  end

  def pre_send_announcement_internal
    pgProvisionedService.all.each do |provisionedservice|
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
    parent = pgProvisionedService.get(name).pgbindusers.all(:default_user => true)[0]
    # connect as the system user (not the parent or any of the
    # children) to ensure we don't have ACL problems
    connection = postgresql_connect @postgresql_config["host"], @postgresql_config["user"], @postgresql_config["pass"], @postgresql_config["port"], name, true
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
    return if pgProvisionedService.get(name).quota_exceeded
    connection = postgresql_connect @postgresql_config["host"], @postgresql_config["user"], @postgresql_config["pass"], @postgresql_config["port"], name, true
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
    return if pgProvisionedService.get(name).quota_exceeded
    connection = postgresql_connect @postgresql_config["host"], @postgresql_config["user"], @postgresql_config["pass"], @postgresql_config["port"], name, true
    raise "Fail to connect to database #{name}" unless connection
    parent = pgProvisionedService.get(name).pgbindusers.all(:default_user => true)[0]
    connection.query("GRANT CREATE ON DATABASE #{name} TO #{parent.user}")
  rescue => x
    @logger.warn("Exception while managing create privilege on database #{name}: #{x}")
  ensure
    connection.close if connection
  end

  def manage_maxconnlimit(name)
    global_connection.query("update pg_database set datconnlimit=#{@max_db_conns} where datname='#{name}' and datconnlimit=-1")
  rescue => x
    @logger.warn("Exception while managing maxconnlimit on database #{name}: #{x}")
  end

  def init_global_connection(instance)
    @connection
  end

  def setup_global_connection(instance)
    @connection
  end

  def fetch_global_connection(name)
    @connection
  end

  def delete_global_connection(name)
    nil
  end

  def global_connection(instance=nil, keepalive=false)
    @connection ||= management_connection
  end

  def management_connection(instance=nil, super_user=true)
    conn = nil
    if instance.nil?
      conn = postgresql_connect(@postgresql_config["host"], @postgresql_config["user"], @postgresql_config["pass"], @postgresql_config["port"], @postgresql_config["database"], false)
    elsif super_user
      # use the super user defined in the configuration file
      db_name = instance
      if instance.is_a?pgProvisionedService
        db_name = instance.name
      end
      conn = postgresql_connect(@postgresql_config["host"], @postgresql_config["user"], @postgresql_config["pass"], @postgresql_config["port"], db_name, true)
    else
      # use the default user of the service_instance
      default_user = instance.pgbindusers.all(:default_user => true)[0]
      conn = postgresql_connect(@postgresql_config["host"], default_user.user, default_user.password, @postgresql_config["port"],instance.name, true) if default_user
    end
    conn
  end

  def node_ready?()
    @connection && connection_exception(@connection).nil?
  end

  #keep connection alive, and check db liveness
  def postgresql_keep_alive
    if connection_exception(@connection)
      @logger.warn("PostgreSQL connection lost, trying to keep alive.")
      @connection = management_connection
    end
  end

  def get_db_stat
    get_db_stat_by_connection(@connection, @max_db_size)
  end

  def get_db_list
    get_db_list_by_connection(@connection)
  end

  def dbs_size(dbs=[])
    dbs = [] if dbs.nil?

    result = {}
    res = global_connection.query('select datname, sum(pg_database_size(datname)) as sum_size from pg_database group by datname')
    res.each do |x|
      name, size = x["datname"], x["sum_size"]
      result[name] = size.to_i
    end

    if dbs.length > 0
      if db.is_a?pgProvisionedService
        name = db.name
      else
        name = db
      end
      dbs.each {|db| result[name] = 0 unless result.has_key? name}
    end
    result
  end

  def postgresql_config(instance=nil)
    unless instance && instance.is_a?(pgProvisionedService) && instance.name
      @postgresql_config
    else
      pc = @postgresql_config.dup
      pc['name'] = instance.name
      pc
    end
  end

  def kill_long_queries
    @long_queries_killed += kill_long_queries_internal(@connection, @postgresql_config['user'], @max_long_query)
  end

  def kill_long_transaction
    @long_tx_killed += kill_long_transaction_internal(@connection, @postgresql_config['user'], @max_long_tx)
  end

  def setup_timers
    EM.add_periodic_timer(VCAP::Services::Postgresql::Node::KEEP_ALIVE_INTERVAL) {postgresql_keep_alive}
    EM.add_periodic_timer(@max_long_query.to_f / 2) {kill_long_queries} if @max_long_query > 0
    EM.add_periodic_timer(@max_long_tx.to_f / 2) {kill_long_transaction} if @max_long_tx > 0
    EM.add_periodic_timer(VCAP::Services::Postgresql::Node::STORAGE_QUOTA_INTERVAL) {enforce_storage_quota}
  end

  def get_inst_port(instance=nil)
    @postgresql_config['port']
  end

  def free_inst_port(port)
    true
  end

  def set_inst_port(instance, credential)
    true
  end

end
