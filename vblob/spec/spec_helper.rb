# Copyright (c) 2009-2011 VMware, Inc.
ENV["BUNDLE_GEMFILE"] ||= File.expand_path("../../Gemfile", __FILE__)
PWD = File.dirname(__FILE__)
TMP = '/tmp/vblob'

$:.unshift File.join(PWD, '..')
$:.unshift File.join(PWD, '..', 'lib')

require "rubygems"
require "bundler/setup"
require "vcap_services_base"
require "rspec"
require "socket"
require "timeout"
require "erb"
require "vblob_service/vblob_node"
require "fileutils"
require 'vcap/common'
require 'vcap/logging'

# Define constants
HTTP_PORT = 9865

TEST_COLL    = 'testColl'
TEST_KEY     = 'test_key'
TEST_VAL     = 1234
TEST_VAL_2   = 4321

include VCAP::Services::VBlob

module VCAP
  module Services
    module VBlob
      class Node
        attr_reader :available_memory
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
  config_file = File.join(PWD, "../config/vblob_node.yml")
  config = YAML.load_file(config_file)
  vblob_conf_template = File.join(PWD, "../resources/vblob.conf.erb")
  options = {
  #  :logger => Logger.new(parse_property(config, "log_file", String, :optional => true) || STDOUT, "daily"),
    :nodejs_path => parse_property(config, "nodejs_path", String),
    :plan => parse_property(config, "plan", String),
    :capacity => parse_property(config, "capacity", Integer),
    :vblobd_path => parse_property(config, "vblobd_path", String),
    :vblobd_log_dir => parse_property(config, "vblobd_log_dir", String),
    :ip_route => parse_property(config, "ip_route", String, :optional => true),
    :node_id => parse_property(config, "node_id", String),
    :mbus => parse_property(config, "mbus", String),
    :config_template => vblob_conf_template,
    :port_range => parse_property(config, "port_range", Range),
    :base_dir => '/tmp/vblob/instances',
    :local_db => 'sqlite3:/tmp/vblob/vblob_node.db'
  }
  VCAP::Logging.setup_from_config(config["logging"])
  # Use the node id for logger identity name.
  options[:logger] = VCAP::Logging.logger(options[:node_id])
  options
end
