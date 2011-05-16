# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), '..')
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')
$LOAD_PATH.unshift(File.expand_path("../../../", __FILE__))
$LOAD_PATH.unshift(File.expand_path("../../lib", __FILE__))

require '../../base/spec/spec_helper'

require "rubygems"
require "rspec"
require "socket"
require "timeout"
require "mongo"
require "erb"

HTTP_PORT = 9865

def is_port_open?(host, port)
  begin
    Timeout::timeout(1) do
      begin
        s = TCPSocket.new(host, port)
        s.close
        return true
      rescue Errno::ECONNREFUSED, Errno::EHOSTUNREACH
        return false
      end
    end
  rescue Timeout::Error
  end
  false
end

def get_backup_dir(backup_dir)
  dir = backup_dir
  # Backup Dir: base_backup/mongodb/ab/cd/ef/uuid/timestamp
  #             base_backup/<6-more-layers>
  6.times do
    dirs = Dir.entries(dir)
    dirs.delete('.')
    dirs.delete('..')
    dir = File.join(dir, dirs[0])
  end
  dir
end

def shutdown(mongodb_node)
    mongodb_node.shutdown
    EM.stop
end


def symbolize_keys(hash)
  if hash.is_a? Hash
    new_hash = {}
    hash.each do |k, v|
      new_hash[k.to_sym] = symbolize_keys(v)
    end
    new_hash
  else
    hash
  end
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

def get_node_config()
  config_file = File.join(File.dirname(__FILE__), "../config/mongodb_node.yml")
  config = YAML.load_file(config_file)
  mongodb_conf_template = File.join(File.dirname(__FILE__), "../resources/mongodb.conf.erb")
  options = {
    :logger => Logger.new(parse_property(config, "log_file", String, :optional => true) || STDOUT, "daily"),
    :mongod_path => parse_property(config, "mongod_path", String),
    :ip_route => parse_property(config, "ip_route", String, :optional => true),
    :available_memory => parse_property(config, "available_memory", Integer),
    :node_id => parse_property(config, "node_id", String),
    :mbus => parse_property(config, "mbus", String),
    :config_template => mongodb_conf_template,
    :port_range => parse_property(config, "port_range", Range),
    :max_memory => parse_property(config, "max_memory", Integer),
    :base_dir => '/tmp/mongo/instances',
    :local_db => 'sqlite3:/tmp/mongo/mongodb_node.db'
  }
  options[:logger].level = Logger::FATAL
  options
end

def get_provisioner_config()
  config_file = File.join(File.dirname(__FILE__), "../config/mongodb_gateway.yml")
  config = YAML.load_file(config_file)
  config = symbolize_keys(config)
  options = {
    :logger => Logger.new(parse_property(config, "log_file", String, :optional => true) || STDOUT, "daily"),
    # Following options are for Provisioner
    :version => config[:service][:version],
    :local_ip => 'localhost',
    :mbus => config[:mbus],
    # Following options are for AsynchronousServiceGateway
    :service => config[:service],
    :token => config[:token],
    :cloud_controller => config[:cloud_controller],
    # Following options are for Thin
    :host => 'localhost',
    :port => HTTP_PORT
  }
  options[:logger].level = Logger::FATAL
  options
end

def start_server(opts)
  sp = Provisioner.new(@opts).start()
  opts = opts.merge({:provisioner => sp})
  sg = VCAP::Services::AsynchronousServiceGateway.new(opts)
  Thin::Server.start(opts[:host], opts[:port], sg)
end




