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

module Boolean; end
class ::TrueClass; include Boolean; end
class ::FalseClass; include Boolean; end

def getLogger()
  logger = Logger.new( STDOUT)
  logger.level = Logger::DEBUG
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
  File.join(File.dirname(__FILE__), '..', 'config')
end

def getNodeTestConfig()
  config_file = File.join(config_base_dir, 'mysql_node.yml')
  config = YAML.load_file(config_file)
  options = {
    :logger => getLogger,
    :base_dir => parse_property(config, "base_dir", String),
    :plan => parse_property(config, "plan", String),
    :capacity => parse_property(config, "capacity", Integer),
    :mysqldump_bin => parse_property(config, "mysqldump_bin", String),
    :mysql_bin => parse_property(config, "mysql_bin", String),
    :gzip_bin => parse_property(config, "gzip_bin", String),
    :mysql_bin => parse_property(config, "mysql_bin", String),
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
  }
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
