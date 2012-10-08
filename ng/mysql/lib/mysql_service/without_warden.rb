module VCAP
  module Services
    module Mysql
      module WithoutWarden
      end
    end
  end
end

module VCAP::Services::Mysql::WithoutWarden
  def mysqlProvisionedService
    VCAP::Services::Mysql::Node::ProvisionedService
  end

  def pre_send_announcement_internal
    @pool_mutex = Mutex.new
    @pools = {}

    # initial pools
    @supported_versions.each do |version|
      config = @mysql_configs[version]
      if not config
        @logger.fatal("Can't find mysql configrations for version:#{version}, exit.")
        exit
      end
      @pools[version] = mysql_connect(config)
    end

    @capacity_lock.synchronize do
      mysqlProvisionedService.all.each do |instance|
        @capacity -= capacity_unit
      end
    end

    mysqlProvisionedService.all.each do |instance|
      setup_pool(instance)
    end
  end

  def handle_provision_exception(provisioned_service)
    delete_database(provisioned_service) if provisioned_service
  end

  def help_unprovision(provisioned_service)
    if not provisioned_service.destroy
      @logger.error("Could not delete service: #{provisioned_service.errors.inspect}")
      raise MysqlError.new(MysqError::MYSQL_LOCAL_DB_ERROR)
    end
    # the order is important, restore quota only when record is deleted from local db.
  end

  def fetch_pool(instance)
    return unless instance
    @pool_mutex.synchronize do
      @pools[instance]
    end
  end

  def get_port(provisioned_service)
    return unless provisioned_service
    @mysql_configs[provisioned_service.version]["port"]
  end

  def each_pool
    mysqlProvisionedService.all.each do |instance|
      conn_pool = fetch_pool(instance.name)
      if conn_pool.nil?
        @logger.warn("no pool for #{instance.inspect}")
        next
      end
      yield conn_pool, instance
    end
  end

  #override new_port to make it do nothing
  def new_port(port=nil)
  end

  def setup_pool(instance)
    return unless instance
    version = instance.version
    @pool_mutex.synchronize do
      # reuse the existing pool for sepcified version.
      pool = @pools[version]
      @pools[instance.name] = pool
    end
  end

  def method_missing(method_name, *args, &block)
    no_ops = [:init_internal ]
    super unless no_ops.include?(method_name)
  end
end
