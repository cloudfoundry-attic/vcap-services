# Copyright (c) 2009-2011 VMware, Inc.

PWD = File.dirname(__FILE__)
TMP = '/tmp/mongo'

$:.unshift File.join(PWD, '..')
$:.unshift File.join(PWD, '..', 'lib')

require "rubygems"
require "rspec"
require 'bundler/setup'
require "vcap_services_base"
require "socket"
require "timeout"
require "mongo"
require "erb"
require "mongodb_service/mongodb_node"
require "fileutils"

# Define constants
HTTP_PORT = 9865

TEST_COLL    = 'testColl'
TEST_KEY     = 'test_key'
TEST_VAL     = 1234
TEST_VAL_2   = 4321

BACKUP_DIR    = File.join(TMP, 'backup')
CONFIG_DIR    = File.join(TMP, 'config')
CONFIG_FILE   = File.join(TMP, 'mongodb_backup.yml')
TEMPLATE_FILE = File.join(PWD, 'config/mongodb_backup.yml.erb')

FileUtils.mkdir_p(BACKUP_DIR)
FileUtils.mkdir_p(CONFIG_DIR)

include VCAP::Services::MongoDB

module VCAP
  module Services
    module MongoDB
      class Node
        attr_reader :available_memory
        attr_accessor :max_clients
      end
    end
  end
end

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

def delete_admin(options)
  db = Mongo::Connection.new('127.0.0.1', options['port']).db(options['db'])
  auth = db.authenticate(options['username'], options['password'])
  db.remove_user('admin')

  db = Mongo::Connection.new('127.0.0.1', options['port']).db('admin')
  service = VCAP::Services::MongoDB::Node::ProvisionedService.get(options['name'])
  auth = db.authenticate(service.admin, service.adminpass)
  db.remove_user(service.admin)
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
  config_file = File.join(PWD, "../config/mongodb_node.yml")
  config = YAML.load_file(config_file)
  mongodb_conf_template = File.join(PWD, "../resources/mongodb.conf.erb")
  options = {
    :logger => Logger.new(parse_property(config, "log_file", String, :optional => true) || STDOUT, "daily"),
    :plan => parse_property(config, "plan", String),
    :capacity => parse_property(config, "capacity", Integer),
    :mongod_path => parse_property(config, "mongod_path", String),
    :mongorestore_path => parse_property(config, "mongorestore_path", String),
    :ip_route => parse_property(config, "ip_route", String, :optional => true),
    :node_id => parse_property(config, "node_id", String),
    :mbus => parse_property(config, "mbus", String),
    :config_template => mongodb_conf_template,
    :port_range => parse_property(config, "port_range", Range),
    :max_memory => parse_property(config, "max_memory", Integer),
    :max_clients => parse_property(config, "max_clients", Integer, :optional => true),
    :base_dir => '/tmp/mongo/instances',
    :mongod_log_dir => '/tmp/mongo/mongod_log',
    :local_db => 'sqlite3:/tmp/mongo/mongodb_node.db'
  }
  options[:logger].level = Logger::FATAL
  options
end
