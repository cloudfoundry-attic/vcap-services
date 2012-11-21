# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), '..')
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'rubygems'
require 'rspec'
require 'bundler/setup'
require "vcap_services_base"
require 'rack/test'
require 'json'
require 'logger'
require 'yaml'

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..')
require 'vcap/common'

def load_config()
  config_file = File.join(File.dirname(__FILE__), '..', 'config', 'service_broker.yml')
  config = YAML.load_file(config_file)
  config = VCAP.symbolize_keys(config)
  config[:host] = "localhost"
  config[:port] ||= VCAP.grab_ephemeral_port
  config[:cloud_controller_uri]  = "api.vcap.me"
  config[:logger] = make_logger()
  config
end

def make_logger()
  logger = Logger.new(STDOUT)
  logger.level = Logger::ERROR
  logger
end
