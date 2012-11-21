# Copyright (c) 2009-2011 VMware, Inc.
ENV["BUNDLE_GEMFILE"] ||= File.expand_path("../../Gemfile", __FILE__)
PWD = File.dirname(__FILE__)

$:.unshift File.join(PWD, '..')
$:.unshift File.join(PWD, '..', 'lib')

require "rubygems"
require "bundler/setup"
require "vcap_services_base"
require "rspec"
require "fileutils"
require 'vcap/common'
require 'vcap/logging'

def get_logger()
  logger = Logger.new( STDOUT)
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
  File.join(File.dirname(__FILE__), '..', 'config')
end

def get_node_test_config()
  config_file = File.join(config_base_dir, 'echo_node.yml')
  config = YAML.load_file(config_file)
  options = {
    :logger => get_logger,
    :plan => parse_property(config, "plan", String),
    :capacity => parse_property(config, "capacity", Integer),
    :node_id => parse_property(config, "node_id", String),
    :mbus => parse_property(config, "mbus", String),
    :local_db => parse_property(config, "local_db", String),
    :ip_route => parse_property(config, "ip_route", String, :optional => true),
    :port => parse_property(config, "port", Integer)
  }
  options
end
