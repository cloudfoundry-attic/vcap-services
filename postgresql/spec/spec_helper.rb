# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), '..')
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'rubygems'
require 'rspec'
require 'bundler/setup'
require 'vcap_services_base'

require 'postgresql_service/util'
require 'postgresql_service/provisioner'
require 'postgresql_service/node'

include VCAP::Services::Postgresql::Util

def getLogger()
  logger = Logger.new( STDOUT)
  logger.level = Logger::ERROR
  return logger
end

def connect_to_postgresql(options)
  host, user, password, port, db =  %w{hostname user password port name}.map { |opt| options[opt] }
  PGconn.connect(host, port, nil, nil, db, user, password)
end

def getNodeTestConfig()
  config_file = File.join(File.dirname(__FILE__), "../config/postgresql_node.yml")
  config = YAML.load_file(config_file)
  options = {
    :logger => getLogger,
    :base_dir => parse_property(config, "base_dir", String),
    :plan => parse_property(config, "plan", String),
    :capacity => parse_property(config, "capacity", Integer),
    :max_db_size => parse_property(config, "max_db_size", Integer),
    :max_long_query => parse_property(config, "max_long_query", Integer),
    :node_id => parse_property(config, "node_id", String),
    :mbus => parse_property(config, "mbus", String),
    :local_db => parse_property(config, "local_db", String),
    :postgresql => parse_property(config, "postgresql", Hash),
    :ip_route => parse_property(config, "ip_route", String, :optional => true),
    :max_long_tx => parse_property(config, "max_long_tx", Integer),
    :max_db_conns => parse_property(config, "max_db_conns", Integer),
    :restore_bin => parse_property(config, "restore_bin", String),
    :dump_bin => parse_property(config, "dump_bin", String),
    :db_size_overhead => parse_property(config, "db_size_overhead", Float)
  }
  options
end

def getProvisionerTestConfig()
  config_file = File.join(File.dirname(__FILE__), "../config/postgresql_gateway.yml")
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
