# Copyright (c) 2009-2012 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), "..")
$:.unshift File.join(File.dirname(__FILE__), "..", "lib")

ENV["RACK_ENV"] = "test"

require "rubygems"
require "bundler"
Bundler.require(:default, :test)

require "simplecov"
require "simplecov-rcov"
class SimpleCov::Formatter::MergedFormatter
  def format(result)
     SimpleCov::Formatter::HTMLFormatter.new.format(result)
     SimpleCov::Formatter::RcovFormatter.new.format(result)
  end
end
SimpleCov.formatter = SimpleCov::Formatter::MergedFormatter
SimpleCov.start

require "rspec"
require "bundler/setup"
require "json"
require "logger"
require "yaml"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "..", "..")

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

def load_appdirect_config()
  config = YAML.load_file(File.join(File.dirname(__FILE__), "..", "config", "marketplace_gateway.yml"))
  config = symbolize_keys(config)
  appdirect_config = YAML.load_file(File.join(File.dirname(__FILE__), "..", "config", "appdirect.yml"))
  appdirect_config = symbolize_keys(appdirect_config)

  config = config.merge(appdirect_config)
  config[:logger] = make_logger()
  config[:host] = VCAP.local_ip(config[:ip_route])
  config[:port] ||= VCAP.grab_ephemeral_port
  config[:url] = "http://#{config[:host]}:#{config[:port]}"

  config
end

def load_test_config()
  config = YAML.load_file(File.join(File.dirname(__FILE__), "..", "config", "test_marketplace_gateway.yml"))
  config = symbolize_keys(config)

  test_config = YAML.load_file(File.join(File.dirname(__FILE__), "..", "config", "test.yml"))
  test_config = symbolize_keys(test_config)

  config = config.merge(test_config)
  config[:logger] = make_logger()

  config
end

def make_logger()
  logger = Logger.new(STDOUT)
  logger.level = Logger::DEBUG
  logger
end

def load_fixture(filename, resp = '{}')
  File.read("#{File.dirname(__FILE__)}/fixtures/#{filename}") rescue resp
end


