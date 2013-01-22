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
        def get_instance(name)
          ProvisionedService.get(name)
        end
        def tmp_dir_ctl(action)
        end
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

def delete_admin(p_service, options)
  db = Mongo::Connection.new(p_service.ip, '27017').db(options['db'])
  auth = db.authenticate(options['username'], options['password'])
  db.remove_user('admin')

  db = Mongo::Connection.new(p_service.ip, '27017').db('admin')
  auth = db.authenticate(p_service.admin, p_service.adminpass)
  db.remove_user(p_service.admin)
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
  File.join(PWD, "../config/")
end

def get_node_config()
  config_file = File.join(config_base_dir, "mongodb_node.yml")
  config = YAML.load_file(config_file)
  mongodb_conf_template = File.join(PWD, "../resources/mongodb.conf.erb")
  options = {
    # miscellaneous configs
    :logger      => Logger.new(parse_property(config, "log_file", String, :optional => true) || STDOUT, "daily"),
    :plan        => parse_property(config, "plan", String),
    :capacity    => parse_property(config, "capacity", Integer),
    :ip_route    => parse_property(config, "ip_route", String, :optional => true),
    :node_id     => parse_property(config, "node_id", String),
    :mbus        => parse_property(config, "mbus", String),
    :port_range  => parse_property(config, "port_range", Range),
    :max_clients => parse_property(config, "max_clients", Integer, :optional => true),

    # parse mongodb wardenized-service control related config
    :service_bin_dir    => parse_property(config, "service_bin_dir", Hash),
    :service_common_dir => parse_property(config, "service_common_dir", String),

    # mongodb instances related configs
    :config_template       => mongodb_conf_template,
    :supported_versions    => parse_property(config, "supported_versions", Array),
    :default_version       => parse_property(config, "default_version", String),
    :service_start_timeout => parse_property(config, "service_start_timeout", Integer),

    # hardcode unit test related directories to /tmp dir
    :base_dir        => '/tmp/mongo/instances',
    :service_log_dir => '/tmp/mongo/logs',
    :local_db        => 'sqlite3:/tmp/mongo/mongodb_node.db',
    :image_dir       => '/tmp/mongo/images',
    :max_disk        => 128,

    # parse mongodb binary related config
    :mongod_path       => parse_property(config, "mongod_path", Hash),
    :mongod_options    => parse_property(config, "mongod_options", Hash),
    :mongorestore_path => parse_property(config, "mongorestore_path", Hash),
    :mongodump_path    => parse_property(config, "mongodump_path", Hash),
  }
  options[:logger].level = Logger::DEBUG
  options
end
