# Copyright (c) 2009-2011 VMware, Inc.
# This code is based on Redis as a Service.

PWD = File.dirname(__FILE__)

$:.unshift File.join(PWD, '..')
$:.unshift File.join(PWD, '..', 'lib')

require "rubygems"
require "rspec"
require 'bundler/setup'
require "vcap_services_base"
require "socket"
require "timeout"
require "erb"
require "fileutils"


def get_hostname(credentials)
  host = credentials['host']
  port = credentials['port'].to_s
  hostname = host + ":" + port
  return hostname
end

def get_connect_info(credentials)
  hostname = get_hostname(credentials)
  username = @credentials['user']
  password = @credentials['password']

  return [hostname, username, password]
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

def config_base_dir()
  config_path = File.join(PWD, "../config/")
  # detect dev_setup
  dev_local = File.expand_path("~/.cloudfoundry_deployment_local")
  if File.exist?(dev_local)
    File.open(dev_local, "r") do |f|
      f.read.match('CLOUD_FOUNDRY_CONFIG_PATH=([[:graph:]]+)')
      config_path = $1
    end
  end
  config_path
end

def get_node_config()
  config_file = File.join(config_base_dir, "memcached_node.yml")
  config = YAML.load_file(config_file)
  memcached_conf_template = File.join(PWD, "../resources/memcached.conf.erb")

  options = {
    :logger => Logger.new(parse_property(config, "log_file", String, :optional => true) || STDOUT, "daily"),
    :plan => parse_property(config, "plan", String),
    :base_dir => parse_property(config, "base_dir", String),
    :memcached_server_path => parse_property(config, "memcached_server_path", String),
    :capacity => 50,
    :node_id => parse_property(config, "node_id", String),
    :port_range => Range.new(5000, 25000),
    :mbus => parse_property(config, "mbus", String),
    :memcached_log_dir => "/tmp/memcached/memcached_log",
    :max_clients => parse_property(config, "max_clients", Integer),
    :memcached_memory => parse_property(config, "memcached_memory", Integer),
    :plan => parse_property(config, "memcached_memory", Integer),
    :local_db => 'sqlite3:/tmp/memcached/memcached_node.db',
    :local_db_file => "/tmp/memcached/memcached_node.db"
  }
  options[:logger].level = Logger::DEBUG
  options
end
