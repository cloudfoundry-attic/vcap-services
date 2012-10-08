# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), '..')
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'rubygems'
require 'rspec'
require 'bundler/setup'
require 'vcap_services_base'
require "mysql_service/util"
require 'mysql_service/provisioner'
require 'mysql_service/node'

require 'mysql_service/with_warden'
# monkey patch of wardenized node
module VCAP::Services::Mysql::WithWarden
  alias_method :pre_send_announcement_internal_ori, :pre_send_announcement_internal
  def pre_send_announcement_internal
    unless @options[:not_start_instances]
      pre_send_announcement_internal_ori
    else
      @pool_mutex = Mutex.new
      @pools = {}
      @logger.info("Not to start instances")
      mysqlProvisionedService.all.each do |instance|
        new_port(instance.port)
        setup_pool(instance)
      end
    end
  end

  def create_missing_pools
    mysqlProvisionedService.all.each do |instance|
      unless @pools.keys.include?(instance.name)
        new_port(instance.port)
        setup_pool(instance)
      end
    end
  end

  alias_method :shutdown_ori, :shutdown
  def shutdown
    if @use_warden && @options[:not_start_instances]
      super
    else
      shutdown_ori
    end
  end
end

module Boolean; end
class ::TrueClass; include Boolean; end
class ::FalseClass; include Boolean; end

def getLogger()
  logger = Logger.new( STDOUT)
  logger.level = Logger::ERROR
  return logger
end

def connect_to_mysql(options)
  host, user, password, port, db =  %w{hostname user password port name}.map { |opt| options[opt] }
  Mysql2::Client.new(:host => host, :username => user, :password => password, :database => db, :port => port)
end

def connection_pool_klass
    VCAP::Services::Mysql::Util::ConnectionPool
end

def parse_property(hash, key, type, options = {})
  obj = hash[key]
  if obj.nil?
    raise "Missing required option: #{key}" unless options[:optional]
    nil
  elsif type == Range
    raise "Invalid Range object: #{obj}" unless obj.kind_of?(Hash)
    first, last = obj["first"], obj["last"]
    raise "Invalid Range object: #{obj}" unless first.kind_of?(Integer) and last.kind_of?(Integer)
    Range.new(first, last)
  else
    raise "Invalid #{type} object: #{obj}" unless obj.kind_of?(type)
    obj
  end
end

def config_base_dir
  ENV["CLOUD_FOUNDRY_CONFIG_PATH"] || File.join(File.dirname(__FILE__), '..', 'config')
end

def getNodeTestConfig()
  config_file = File.join(config_base_dir, 'mysql_node.yml')
  config = YAML.load_file(config_file)
  options = {
    :logger => getLogger,
    :base_dir => parse_property(config, "base_dir", String),
    :plan => parse_property(config, "plan", String),
    :capacity => parse_property(config, "capacity", Integer),
    :gzip_bin => parse_property(config, "gzip_bin", String),
    :max_db_size => parse_property(config, "max_db_size", Integer),
    :max_long_query => parse_property(config, "max_long_query", Integer),
    :node_id => parse_property(config, "node_id", String),
    :mbus => parse_property(config, "mbus", String),
    :local_db => parse_property(config, "local_db", String),
    :mysql => parse_property(config, "mysql", Hash),
    :ip_route => parse_property(config, "ip_route", String, :optional => true),
    :max_long_tx => parse_property(config, "max_long_tx", Integer),
    :kill_long_tx => parse_property(config, "kill_long_tx", Boolean),
    :max_user_conns => parse_property(config, "max_user_conns", Integer, :optional => true),
    :connection_wait_timeout => 10,
    :disk_overhead => parse_property(config, "disk_overhead", Float, :default => 0.0),
    :use_warden => parse_property(config, "use_warden", Boolean),
    :config_template => File.expand_path("../../resources/my.conf.erb", __FILE__),
    :supported_versions => parse_property(config, "supported_versions", Array),
  }
  if options[:use_warden]
    warden_config = parse_property(config, "warden", Hash, :optional => true)
    options[:log_dir] = parse_property(warden_config, "log_dir", String)
    options[:image_dir] = parse_property(warden_config, "image_dir", String)
    options[:port_range] = parse_property(warden_config, "port_range", Range)
    options[:service_start_timeout] = parse_property(warden_config, "service_start_timeout", Integer, :optional => true, :default => 3)
    options[:filesystem_quota] = parse_property(warden_config, "filesystem_quota", Boolean, :optional => true)
    options[:max_heap_table_size] = parse_property(warden_config, "max_heap_table_size", Integer, :optional => true)
    options[:micro] = parse_property(warden_config, "micro", Boolean, :optional => true)
    options[:production] = parse_property(warden_config, "production", Boolean, :optional => true)
  else
    options[:ip_route] = "127.0.0.1"
  end
  options
end

def getProvisionerTestConfig()
  config_file = File.join(config_base_dir, 'mysql_gateway.yml')
  config = YAML.load_file(config_file)
  config = VCAP.symbolize_keys(config)
  options = {
    :logger   => getLogger,
    :version  => config[:service][:version],
    :local_ip => config[:host],
    :plan_management => config[:plan_management],
    :mbus => config[:mbus]
  }
  options
end

def new_node(options)
  opts = options.dup
  opts[:not_start_instances] = true if opts[:use_warden]
  VCAP::Services::Mysql::Node.new(opts)
end


def provision_instance
end
