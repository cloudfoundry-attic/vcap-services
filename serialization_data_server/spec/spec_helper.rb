# Copyright (c) 2009-2011 VMware, Inc.

require 'rubygems'
require 'rspec'
require 'redis'
require 'mock_redis'
require 'tempfile'
require 'logger'

class Redis
  def self.connect(options = {})
    return MockRedis.new
  end
end

$:.unshift File.join(File.dirname(__FILE__), '..')
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')
require 'server'
require 'store'


def load_config()
  config_file = File.join(File.dirname(__FILE__), '..', 'config', 'serialization_data_server.yml')
  config = YAML.load_file(config_file)
  config = VCAP.symbolize_keys(config)
  config[:redis][:host] ||= "localhost"
  config[:redis][:port] ||= VCAP.grab_ephemeral_port
  config[:redis][:password] ||= "redispasswd"
  config[:port] ||= VCAP.grab_ephemeral_port
  config[:cloud_controller_uri]  ||= "api.vcap.me"
  config[:external_uri] ||= "dl.vcap.me"
  config[:logger] = getLogger()
  config[:upload_token] = "uploadtoken"
  config
end

def getLogger()
  logger = Logger.new(STDOUT)
  logger.level = Logger::DEBUG
  return logger
end
