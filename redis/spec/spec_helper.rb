# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), "..")
$:.unshift File.join(File.dirname(__FILE__), "..", "lib")

ENV["BUNDLE_GEMFILE"] ||= File.expand_path("../../Gemfile", __FILE__)

require "rubygems"
require "rspec"
require "bundler/setup"
require "vcap_services_base"
require "nats/client"
require "vcap/common"
require "datamapper"
require "uri"
require "redis"
require "thread"
require "redis_service/redis_node"
require "redis_service/redis_error"

def getLogger
  logger = Logger.new(STDOUT)
  logger.level = Logger::DEBUG
  logger
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
  ENV["CLOUD_FOUNDRY_CONFIG_PATH"] || File.join(File.dirname(__FILE__), "..", "config")
end

def getNodeTestConfig
  config_file = File.join(config_base_dir, "redis_node.yml")
  config = YAML.load_file(config_file)
  options = {
    :logger => getLogger,
    :base_dir => "/tmp/redis_instances",
    :plan => parse_property(config, "plan", String),
    :capacity => parse_property(config, "capacity", Integer),
    :node_id => parse_property(config, "node_id", String),
    :mbus => parse_property(config, "mbus", String),
    :local_db_file => "/tmp/redis_node_" + Time.now.to_i.to_s + ".db",
    :ip_route => parse_property(config, "ip_route", String, :optional => true),
    :redis_server_path => parse_property(config, "redis_server_path", String),
    :config_template => File.join(File.dirname(__FILE__), "..", "resources/redis.conf.erb"),
    :port_range => parse_property(config, "port_range", Range),
    :max_memory => parse_property(config, "max_memory", Integer),
    :max_swap => parse_property(config, "max_swap", Integer),
    :redis_log_dir => "/tmp/redis_log",
    :command_rename_prefix => parse_property(config, "command_rename_prefix", String),
    :max_clients => parse_property(config, "max_clients", Integer, :optional => true),
    :image_dir => "/tmp/redis_image",
    :max_db_size => parse_property(config, "max_db_size", Integer),
    :migration_nfs => "/tmp/migration",
  }
  options[:local_db] = "sqlite3:" + options[:local_db_file]
  options
end

def redis_echo(host, port, password=nil)
  if password
    redis = Redis.new({:host => host, :port => port, :password => password})
  else
    redis = Redis.new({:host => host, :port => port})
  end
  redis.echo("")
  redis.quit
  true
end

def redis_set(host, port, password, key, value)
  redis = Redis.new({:host => host, :port => port, :password => password})
  redis.set(key, value)
  redis.quit if redis
  true
end

def redis_get(host, port, password, key)
  redis = Redis.new({:host => host, :port => port, :password => password})
  value = redis.get(key)
  redis.quit if redis
  value
end

def redis_del(host, port, password, key)
  redis = Redis.new({:host => host, :port => port, :password => password})
  value = redis.del(key)
  redis.quit if redis
  true
end

def redis_save(host, port, password, command)
  redis = Redis.new({:host => host, :port => port, :password => password})
  redis.save(command)
  redis.quit if redis
  true
end
