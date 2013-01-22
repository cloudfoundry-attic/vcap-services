# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), '..')
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

ENV["RACK_ENV"] = "test"

require "rubygems"
require "bundler"
Bundler.require(:default, :test)

require 'rspec'
require 'bundler/setup'
require 'json'
require 'logger'
require 'yaml'

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..')
require 'vcap/common'

module VCAP
  module Services
    module ServiceBroker
      class AsynchronousServiceGateway < VCAP::Services::BaseAsynchronousServiceGateway

        # Helpers for unit testing
        get "/" do
          return { "gateway" => "ServiceBroker" }.to_json
        end
      end
    end
  end
end


def load_config()
  config_file = File.join(File.dirname(__FILE__), '..', 'config', 'service_broker.yml')
  config = YAML.load_file(config_file)
  config = VCAP.symbolize_keys(config)
  config[:host] = "localhost"
  config[:port] ||= VCAP.grab_ephemeral_port
  config[:cloud_controller_uri]  = "api.vcap.me"
  config[:logger] = make_logger()
  config[:cc_api_version] = "v1"
  config
end

def make_logger()
  logger = Logger.new(STDOUT)
  logger.level = Logger::DEBUG
  logger
end
