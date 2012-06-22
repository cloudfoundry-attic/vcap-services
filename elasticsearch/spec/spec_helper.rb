# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), '..')
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')
$LOAD_PATH.unshift(File.expand_path("../../../", __FILE__))
$LOAD_PATH.unshift(File.expand_path("../../lib", __FILE__))
require "rubygems"
require "rspec"
require 'bundler/setup'
require 'vcap_services_base'
require "socket"
require "timeout"
require "rest-client"
require "elasticsearch_service/elasticsearch_node"

include VCAP::Services::ElasticSearch

def is_port_open?(host, port)
  begin
    Timeout::timeout(1) do
      begin
        TCPSocket.new(host, port).close
        true
      rescue Errno::ECONNREFUSED, Errno::EHOSTUNREACH => e
        false
      end
    end
  rescue Timeout::Error => e
    false
  end
end

def get_logger
  logger = Logger.new(STDOUT)
  logger.level = Logger::ERROR
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
  ENV["CLOUD_FOUNDRY_CONFIG_PATH"] || File.join(File.dirname(__FILE__), '..', 'config')
end

def get_node_config
  config_file = File.join(config_base_dir, "elasticsearch_node.yml")
  config = YAML.load_file(config_file)
  config_template = File.join(File.dirname(__FILE__), "../resources/elasticsearch.yml.erb")
  logging_config_file = File.join(File.dirname(__FILE__), "../resources/logging.yml")
  options = {
    :logger => get_logger,
    :elasticsearch_log_dir => '/tmp/elasticsearch/logs',
    :elasticsearch_path => parse_property(config, "elasticsearch_path", String),
    :elasticsearch_plugin_dir => parse_property(config, "elasticsearch_plugin_dir", String),
    :node_id => parse_property(config, "node_id", String),
    :mbus => parse_property(config, "mbus", String),
    :config_template => config_template,
    :logging_config_file => logging_config_file,
    :port_range => parse_property(config, "port_range", Range),
    :max_memory => parse_property(config, "max_memory", Integer),
    :capacity => parse_property(config, "capacity", Integer),
    :base_dir => '/tmp/elasticsearch/instances',
    :local_db => 'sqlite3:/tmp/elasticsearch/elasticsearch_node.db',
    :pid => '/tmp/elasticsearch/elasticsearch_node.pid'
  }
  options
end
