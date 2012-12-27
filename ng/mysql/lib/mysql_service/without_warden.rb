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

  def pre_send_announcement_internal(options)
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
    @pool_mutex.synchronize { @pools.delete(provisioned_service.name) }
  end

  def help_unprovision(provisioned_service)
    @pool_mutex.synchronize { @pools.delete(provisioned_service.name) }
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

  def each_pool_with_identifier
    @supported_versions.each do |version|
      yield @pools[version], version
    end
  end

  def extract_attr(identifier, attribute) #identifier is version
    case attribute
    when :port then @mysql_configs[identifier]["port"]
    when :key  then identifier
    end
  end

  def extra_size_per_db(connection, dbs_size)
    avg_factor = @max_capacity
    @capacity_lock.synchronize do
       avg_factor -= @capacity if @capacity < 0
    end
    system_and_extra_size(connection, dbs_size) / avg_factor
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
    no_ops = [:init_internal]
    super unless no_ops.include?(method_name)
  end
end
