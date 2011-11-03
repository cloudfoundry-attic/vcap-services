# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), '..')
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'rubygems'
require 'rspec'

require "mysql_service/util"
require 'mysql_service/provisioner'
require 'mysql_service/node'

include VCAP::Services::Mysql::Util

def getLogger()
  logger = Logger.new( STDOUT)
  logger.level = Logger::ERROR
  return logger
end

def connect_to_mysql(options)
  host, user, password, port, db =  %w{hostname user password port name}.map { |opt| options[opt] }
  Mysql.real_connect(host, user, password, db, port)
end

def getNodeTestConfig()
  config_file = File.join(File.dirname(__FILE__), "../config/mysql_node.yml")
  config = YAML.load_file(config_file)
  options = {
    :logger => getLogger,
    :base_dir => parse_property(config, "base_dir", String),
    :mysqldump_bin => parse_property(config, "mysqldump_bin", String),
    :mysql_bin => parse_property(config, "mysql_bin", String),
    :gzip_bin => parse_property(config, "gzip_bin", String),
    :mysql_bin => parse_property(config, "mysql_bin", String),
    :available_storage => parse_property(config, "available_storage", Integer),
    :max_db_size => parse_property(config, "max_db_size", Integer),
    :max_long_query => parse_property(config, "max_long_query", Integer),
    :node_id => parse_property(config, "node_id", String),
    :mbus => parse_property(config, "mbus", String),
    :local_db => parse_property(config, "local_db", String),
    :mysql => parse_property(config, "mysql", Hash),
    :ip_route => parse_property(config, "ip_route", String, :optional => true),
    :max_long_tx => parse_property(config, "max_long_tx", Integer),
    :max_user_conns => parse_property(config, "max_user_conns", Integer, :optional => true),
  }
  options
end

def getProvisionerTestConfig()
  config_file = File.join(File.dirname(__FILE__), "../config/mysql_gateway.yml")
  config = YAML.load_file(config_file)
  config = VCAP.symbolize_keys(config)
  options = {
    :logger   => getLogger,
    :version  => config[:service][:version],
    :local_ip => config[:host],
    :mbus => config[:mbus]
  }
  options
end
