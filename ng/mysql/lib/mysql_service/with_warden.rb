module VCAP
  module Services
    module Mysql
      module WithWarden
      end
    end
  end
end

module VCAP::Services::Mysql::WithWarden
  def mysqlProvisionedService
    VCAP::Services::Mysql::Node::WardenProvisionedService
  end

  def init_internal(options)
    @service_start_timeout = @options[:service_start_timeout] || 3
    init_ports(options[:port_range])
  end

  def pre_send_announcement_internal(options)
    @pool_mutex = Mutex.new
    @pools = {}

    start_all_instances
    @capacity_lock.synchronize{ @capacity -= mysqlProvisionedService.all.size }

    mysqlProvisionedService.all.each do |instance|
      setup_pool(instance)
    end
    warden_node_init(options)
  end

  def handle_provision_exception(provisioned_service)
    return unless provisioned_service
    name = provisioned_service.name
    @pool_mutex.synchronize do
      @pools[name].shutdown
      @pools.delete(name)
    end if @pools.has_key?(name)
    free_port(provisioned_service.port)
    provisioned_service.delete
  end

  def get_port(provisioned_service)
    provisioned_service.port
  end

  def help_unprovision(provisioned_service)
    name = provisioned_service.name
    @pool_mutex.synchronize do
      @pools[name].shutdown
      @pools.delete(name)
    end
    free_port(provisioned_service.port)
    raise "Could not cleanup instance #{provisioned_service.name}" unless provisioned_service.delete
  end

  def is_service_started(instance)
    get_status(instance) == "ok"
  end

  def shutdown
    super
    @logger.info("Shutting down instances..")
    stop_all_instances
  end

  def setup_pool(instance)
    return unless instance
    config = @mysql_configs[instance.version].clone
    config["host"] = instance.ip
    conn = mysql_connect(config, false)
    raise "Setup pool failed: can't connection to #{config}" unless conn

    @pool_mutex.synchronize do
      @pools[instance.name] = conn
    end
    conn
  end

  def fetch_pool(key)
    return unless key
    @pool_mutex.synchronize do
      @pools[key]
    end
  end

  def each_pool_with_identifier
    # we can't iterate using @pools.each because provision and unprovision
    # will change @pools. Changing @pools during @pools.each will cause an error
    mysqlProvisionedService.all.each do |instance|
      conn_pool = fetch_pool(instance.name)
      next if conn_pool.nil?
      yield conn_pool, instance
    end
  end

  def extract_attr(identifier, attribute) #identifier is instance
    case attribute
    when :port then identifier.port
    when :key  then identifier.name
    end
  end

  def extra_size_per_db(connection, dbs_size)
    system_and_extra_size(connection, dbs_size)
  end
end
