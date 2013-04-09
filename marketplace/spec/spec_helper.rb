# Copyright (c) 2009-2012 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), "..")
$:.unshift File.join(File.dirname(__FILE__), "..", "lib")

ENV["RACK_ENV"] = "test"

require "rubygems"
require "bundler"
Bundler.require(:default, :test)

require "rspec"
require "bundler/setup"
require "json"
require "logger"
require "yaml"
require "fileutils"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "..", "..")

module VCAP
  module Services
    module Marketplace
      class MarketplaceServiceGateway < VCAP::Services::BaseAsynchronousServiceGateway

        # Helpers for unit testing
        get "/" do
          return {"marketplace" => @marketplace_client.name, "offerings" => @marketplace_client.get_catalog}.to_json
        end

        post "/marketplace/set/:key/:value" do
          @logger.info("TEST HELPER ENDPOINT - set: key=#{params[:key]}, value=#{params[:value]}")
          Fiber.new {
            begin
              @marketplace_client.set_config(params[:key], params[:value])
              refresh_catalog_and_update_cc(true)
              async_reply("")
            rescue => e
              reply_error(e.inspect)
            end
          }.resume
          async_mode
        end
      end
    end
  end
end

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

def load_config(marketplace_name)
  config = YAML.load_file(File.join(File.dirname(__FILE__), "..", "config", "marketplace_gateway.yml"))
  config = symbolize_keys(config)

  marketplace_config = YAML.load_file(File.join(File.dirname(__FILE__), "..", "config", "#{marketplace_name}.yml"))
  marketplace_config = symbolize_keys(marketplace_config)

  config = config.merge(marketplace_config)
  config[:logger] = make_logger()

  config
end

def make_logger()
  logger = Logger.new(STDOUT)
  logger.level = Logger::DEBUG
  logger
end

def null_object
  double('null object').as_null_object
end
