# Copyright (c) 2009-2011 VMware, Inc.

PWD = File.dirname(__FILE__)
TMP = '/tmp/couchdb'

$:.unshift File.join(PWD, '..')
$:.unshift File.join(PWD, '..', 'lib')

require "rubygems"
require "rspec"
require "socket"
require "timeout"
require "couchrest"
require "erb"
require "vcap_services_base"
require "couchdb_service/couchdb_node"
require "couchdb_service/util"
require "fileutils"

# Define constants
HTTP_PORT = 9865

TEST_COLL    = 'testColl'
TEST_KEY     = 'test_key'
TEST_VAL     = 1234
TEST_VAL_2   = 4321

BACKUP_DIR    = File.join(TMP, 'backup')
CONFIG_DIR    = File.join(TMP, 'config')
CONFIG_FILE   = File.join(TMP, 'couchdb_backup.yml')
TEMPLATE_FILE = File.join(PWD, 'config/couchdb_backup.yml.erb')

FileUtils.mkdir_p(BACKUP_DIR)
FileUtils.mkdir_p(CONFIG_DIR)

include VCAP::Services::CouchDB
include VCAP::Services::CouchDB::Util

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

def get_backup_dir(backup_dir)
  dir = backup_dir
  # Backup Dir: base_backup/couchdb/ab/cd/ef/uuid/timestamp
  #             base_backup/<6-more-layers>
  6.times do
    dirs = Dir.entries(dir)
    dirs.delete('.')
    dirs.delete('..')
    dir = File.join(dir, dirs[0])
  end
  dir
end

def delete_admin(config, options)
  server = CouchRest.new("http://#{config["admin"]}:#{config["adminpass"]}@localhost:#{options['port']}")
  auth_db = server.database("_users")
  auth_db_url = "#{server.uri}#{auth_db.uri}"

  service = VCAP::Services::CouchDB::Node::ProvisionedService.get(options['name'])
  key = "org.couchdb.user:#{service.user}"
  admin = auth_db.get(key)
  auth_db.delete_doc(admin)
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
  config_file = File.join(config_base_dir, "couchdb_node.yml")
  config = YAML.load_file(config_file)
  couchdb_conf_template = File.join(PWD, "../resources/local.ini.erb")

  options = {
    :logger => Logger.new(STDOUT, "daily"),
    :plan => parse_property(config, "plan", String),
    :ip_route => parse_property(config, "ip_route", String, :optional => true),
    :capacity => parse_property(config, "capacity", Integer),
    :node_id => parse_property(config, "node_id", String),
    :mbus => "nats://localhost:4222", #parse_property(config, "mbus", String),
    :base_dir => '/tmp/couchdb/instances',
    :local_db_file => "/tmp/couchdb_node_" + Time.now.to_i.to_s + ".db",
    :couchdb => parse_property(config, "couchdb", Hash),
    :couchdb_log_dir => parse_property(config, "couchdb_log_dir", String),
    :couchdb_install_path => parse_property(config, "couchdb_install_path", String),
  }

  options[:local_db] = "sqlite3:" + options[:local_db_file]
  options[:logger].level = Logger::DEBUG

  @couchdb_ctl = "#{options[:couchdb_install_path]}/etc/init.d/couchdb"
  @logger = options[:logger]

  options
end

def delete_leftover_users
  conn = server_admin_connection
  db = conn.database("_users")
  users = db.documents["rows"].select { |u| u["key"] =~ /^org.couchdb.user:/ }
  if users.any?
    # don't warn in before(:all)
    if self.example
      STDERR.puts "WARNING: a spec example (#{self.example.full_description}) left #{users.length} users around, deleting them now"
    end
    users.each do |u|
      db.delete_doc({"_id" => u["id"], "_rev" => u["value"]["rev"]}, true)
    end
    db.bulk_save
  end
end

def server_connection(opts)
  _server_connection(opts['host'], opts['port'], opts['username'], opts['password'])
end


def couchdb_running?(couchdb_ctl)
  couch_status_cmd = "#{couchdb_ctl} status"
  stdin,stdout,stderr = Open3.popen3(couch_status_cmd)
  status_out = stdout.gets
  status_err = stderr.gets

  false if status_err =~ /not running/
  true if status_out =~ /is running/
end

def start_couchdb_server(couchdb_ctl)
  if !couchdb_running?(couchdb_ctl)
    `#{couchdb_ctl} start`
    sleep (3.0) # Sleep 3 sec to allow correct startup
  end
end

def stop_couchdb_server(couchdb_ctl)
  if couchdb_running?(couchdb_ctl)
    `#{couchdb_ctl} stop`
    sleep (3.0) # Sleep 3 sec to allow correct shutdown
  end
end
