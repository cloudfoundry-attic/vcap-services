# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), '..')
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'rubygems'
require 'rspec'
require 'bundler/setup'
require 'vcap_services_base'

require 'postgresql_service/util'
require 'postgresql_service/provisioner'
require 'postgresql_service/with_warden'

# monkey patch of wardenized node
module VCAP::Services::Postgresql::WithWarden
  alias_method :pre_send_announcement_internal_ori, :pre_send_announcement_internal
  def pre_send_announcement_internal(options)
    unless @use_warden && @options[:not_start_instances]
      pre_send_announcement_internal_ori(options)
    else
      @logger.info("Not to start instances")
      pool_run(pgProvisionedService.all.map { |inst| inst }) do |provisionedservice,  _|
        setup_global_connection(provisionedservice)
      end
    end
  end
end

# monkey patch to support page cache cleaner test
class File
  class << self
   def init_fadvise_files
     @fadvise_files = []
   end
   attr_reader :fadvise_files
  end

  alias_method :fadvise_ori, :fadvise
  def fadvise(start, len, advise_symbol)
    self.class.fadvise_files << self.path
  end
end

# monkey patch to PGDBconn to record checkpoint number
class  VCAP::Services::Postgresql::Util::PGDBconn
  attr_reader :checkpoint_times

  alias_method :query_internal_ori, :query_internal
  def query_internal(sql)
    if sql.downcase.strip == "checkpoint"
      @checkpoint_times ||= 0
      @checkpoint_times += 1
    end
    query_internal_ori(sql)
  end
end

module Boolean;end
class ::TrueClass; include Boolean; end
class ::FalseClass; include Boolean; end

def getLogger()
  logger = Logger.new(STDOUT)
  logger.level = Logger::ERROR
  return logger
end

def connect_to_postgresql(options, extra_opts={})
  host, user, password, port, db =  %w{hostname user password port name}.map { |opt| options[opt] }
  VCAP::Services::Postgresql::Util::PGDBconn.new({
    :host => host,
    :port => port,
    :user => user,
    :password => password,
    :dbname => db }.merge!(extra_opts)
  )
end

def config_base_dir
  File.join(File.dirname(__FILE__), '..', 'config')
end

def getNodeTestConfig()
  config_file = File.join(config_base_dir, "postgresql_node.yml")
  config = YAML.load_file(config_file)
  options = {
    :logger => getLogger,
    :base_dir => parse_property(config, "base_dir", String),
    :plan => parse_property(config, "plan", String),
    :capacity => parse_property(config, "capacity", Integer),
    :max_disk => parse_property(config, "max_disk", Numeric, :optional => true, :default => 128),
    :max_db_size => parse_property(config, "max_db_size", Numeric),
    :max_long_query => parse_property(config, "max_long_query", Integer),
    :node_id => parse_property(config, "node_id", String),
    :mbus => parse_property(config, "mbus", String),
    :local_db => parse_property(config, "local_db", String),
    :postgresql => parse_property(config, "postgresql", Hash),
    :ip_route => parse_property(config, "ip_route", String, :optional => true),
    :max_long_tx => parse_property(config, "max_long_tx", Integer),
    :max_db_conns => parse_property(config, "max_db_conns", Integer),
    :use_warden => parse_property(config, "use_warden", Boolean, :optional => true, :default => false),
    :supported_versions => parse_property(config, "supported_versions", Array),
    :default_version => parse_property(config, "default_version", String),
    :disabled_file => parse_property(config, "disabled_file", String, :optional => true, :default => "/var/vcap/store/DISABLED"),
    :db_connect_timeout => parse_property(config, "db_connect_timeout", Integer, :optional => true, :default => 3),
    :db_query_timeout => parse_property(config, "db_query_timeout", Integer, :optional => true, :default => 10),
    :db_use_async_query => parse_property(config, "db_use_async_query", Boolean, :optional => true, :default => true),
    :enable_xlog_enforcer => parse_property(config, "enable_xlog_enforcer", Boolean, :optional => true, :default => false),
    :xlog_enforce_tolerance => parse_property(config, "xlog_enforce_tolerance", Integer, :optional => true, :default => 5),
  }
  if options[:use_warden]
    warden_config = parse_property(config, "warden", Hash, :optional => true)
    options[:use_warden] = true
    options[:service_log_dir] = parse_property(warden_config, "service_log_dir", String)
    options[:port_range] = parse_property(warden_config, "port_range", Range)
    options[:image_dir] = parse_property(warden_config, "image_dir", String)
    options[:filesystem_quota] = parse_property(warden_config, "filesystem_quota", Boolean, :optional => true)
    options[:service_start_timeout] = parse_property(warden_config, "service_start_timeout", Integer, :optional => true, :default => 3)
    options[:service_log_dir] = parse_property(warden_config, "service_log_dir", String)
    options[:service_bin_dir] = parse_property(warden_config, "service_bin_dir", Hash, :optional => true)
    options[:service_common_dir] = parse_property(warden_config, "service_common_dir", String, :optional => true, :default => "/var/vcap/store/postgresql_common")
  else
    options[:ip_route] = "127.0.0.1"
  end
  options
end


def getProvisionerTestConfig()
  config_file = File.join(config_base_dir, "postgresql_gateway.yml")
  config = YAML.load_file(config_file)
  config = VCAP.symbolize_keys(config)
  options = {
    :logger   => getLogger,
    :version  => config[:service][:version],
    :local_ip => config[:host],
    :plan_management => config[:plan_management],
    :mbus => config[:mbus],
    :cc_api_version => "v1",
  }
  options
end

def parse_property(hash, key, type, options = {})
  obj = hash[key]
  if obj.nil?
    raise "Missing required option: #{key}" unless options[:optional]
    options[:default]
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
