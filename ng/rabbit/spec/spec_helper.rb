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
require "bunny"
require "amqp"
require "thread"
require "rabbit_service/rabbit_node"
require "rabbit_service/rabbit_error"

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
  File.join(File.dirname(__FILE__), "..", "config")
end

def loadWorkerTestConfig(local_db_file)
  config_file = File.join(config_base_dir, "rabbit_worker.yml")
  config = YAML.load_file(config_file)
  config[:local_db] = "sqlite3:" + local_db_file
  ENV['WORKER_CONFIG'] = Yajl::Encoder.encode(config)
end

def getNodeTestConfig
  config_file = File.join(config_base_dir, "rabbit_node.yml")
  # The configuration file name could be "rabbitmq_node.yml" in dev_setup environment
  if !File.exist?(config_file)
    config_file = File.join(config_base_dir, "rabbitmq_node.yml")
  end
  config = YAML.load_file(config_file)
  options = {
    # micellaneous configs
    :logger     => getLogger,
    :plan       => parse_property(config, "plan", String),
    :capacity   => parse_property(config, "capacity", Integer),
    :ip_route   => parse_property(config, "ip_route", String, :optional => true),
    :node_id    => parse_property(config, "node_id", String),
    :mbus       => parse_property(config, "mbus", String),
    :port_range => parse_property(config, "port_range", Range),

    # parse rabbitmq wardenized-service control related config
    :service_bin_dir    => parse_property(config, "service_bin_dir", Hash),
    :service_common_dir => parse_property(config, "service_common_dir", String),

    # rabbitmq related configs
    :rabbit                   => parse_property(config, "rabbit", Hash),
    :supported_versions       => parse_property(config, "supported_versions", Array),
    :default_version          => parse_property(config, "default_version", String),
    :max_clients              => parse_property(config, "max_clients", Integer, :optional => true),
    :service_start_timeout    => parse_property(config, "service_start_timeout", Integer, :optional => true),
    :vm_memory_high_watermark => parse_property(config, "vm_memory_high_watermark", Float, :optional => true),
    :bandwidth_per_second     => parse_property(config, "bandwidth_per_second", Float),

    # hardcode unit test related directories to /tmp dir
    :base_dir        => "/tmp/rabbitmq_instances",
    :local_db_file   => "/tmp/rabbitmq_node_" + Time.now.to_i.to_s + ".db",
    :service_log_dir => "/tmp/rabbitmq_instances/log",
    :image_dir       => "/tmp/rabbitmq_image",
    :max_disk        => 10,
    :migration_nfs   => "/tmp/migration",
    :disabled_file   => "/tmp/redis_instances/DISABLED",
    :filesystem_quota => true,
  }
  options[:local_db] = "sqlite3:" + options[:local_db_file]
  options
end

def amqp_start(credentials, instance)
  conn = Bunny.new(:host => instance.ip,
                   :port => 10001,
                   :vhost => credentials["vhost"],
                   :user => credentials["user"],
                   :pass => credentials["pass"])
  conn.start
  conn.close
  true
rescue
  false
end

def amqp_new_queue(credentials, instance, exchange_name, queue_name)
  conn = Bunny.new(:host => instance.ip,
                   :port => 10001,
                   :vhost => credentials["vhost"],
                   :user => credentials["user"],
                   :pass => credentials["pass"])
  conn.start
  ch = conn.create_channel
  q = ch.queue(queue_name, :durable => true, :auto_delete => false)
  x  = ch.direct(exchange_name, :durable => true, :auto_delete => false)
  conn.close
end

def amqp_clear_queue(credentials, instance, exchange_name, queue_name)
  conn = Bunny.new(:host => instance.ip,
                   :port => 10001,
                   :vhost => credentials["vhost"],
                   :user => credentials["user"],
                   :pass => credentials["pass"])
  conn.start
  ch = conn.create_channel
  q = ch.queue(queue_name, :durable => true, :auto_delete => false)
  q.delete
  x  = ch.direct(exchange_name, :durable => true, :auto_delete => false)
  x.delete
  conn.close
end

def amqp_queue_exist?(credentials, queue_name)
  ret = @node.list_queues(credentials, nil)
  ret.each do |q|
    return true if q["name"] == queue_name
  end
  false
end

def amqp_exchange_exist?(credentials, exchange_name)
  ret = @node.list_exchanges(credentials, nil)
  ret.each do |e|
    return true if e["name"] == exchange_name
  end
  false
end

def amqp_connect(credentials, instance)
  EM.run do
    AMQP.connect(:host => instance.ip,
                 :port => 10001,
                 :vhost => credentials["vhost"],
                 :user => credentials["user"],
                 :pass => credentials["pass"])
  end
end
