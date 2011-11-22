# Copyright (c) 2009-2011 VMware, Inc.
ENV["BUNDLE_GEMFILE"] ||= File.expand_path("../../Gemfile", __FILE__)
PWD = File.dirname(__FILE__)
TMP = '/tmp/blob'

$:.unshift File.join(PWD, '..')
$:.unshift File.join(PWD, '..', 'lib')

require "rubygems"
require "bundler/setup"
require "vcap_services_base"
require "rspec"
require "socket"
require "timeout"
require "erb"
require "blob_service/blob_node"
require "fileutils"
require 'vcap/common'
require 'vcap/logging'

# Define constants
HTTP_PORT = 9865

TEST_COLL    = 'testColl'
TEST_KEY     = 'test_key'
TEST_VAL     = 1234
TEST_VAL_2   = 4321

include VCAP::Services::Blob

module VCAP
  module Services
    module Blob
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
  config_file = File.join(PWD, "../config/blob_node.yml")
  config = YAML.load_file(config_file)
  blob_conf_template = File.join(PWD, "../resources/blob.conf.erb")
  options = {
  #  :logger => Logger.new(parse_property(config, "log_file", String, :optional => true) || STDOUT, "daily"),
    :nodejs_path => parse_property(config, "nodejs_path", String),
    :blobd_path => parse_property(config, "blobd_path", String),
    :blobd_log_dir => parse_property(config, "blobd_log_dir", String),
    :ip_route => parse_property(config, "ip_route", String, :optional => true),
    :available_memory => parse_property(config, "available_memory", Integer),
    :node_id => parse_property(config, "node_id", String),
    :mbus => parse_property(config, "mbus", String),
    :config_template => blob_conf_template,
    :port_range => parse_property(config, "port_range", Range),
    :max_memory => parse_property(config, "max_memory", Integer),
    :base_dir => '/tmp/blob/instances',
    :local_db => 'sqlite3:/tmp/blob/blob_node.db'
  }
  VCAP::Logging.setup_from_config(config["logging"])
  # Use the node id for logger identity name.
  options[:logger] = VCAP::Logging.logger(options[:node_id])
  options
end
