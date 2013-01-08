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

class SDSTests
  PORT=34512
  def self.createSDS(opts)
    MockSDS.new(opts)
  end

  class MockSDS
    attr_accessor :response
    attr_accessor :response_url
    def initialize(opts)
      sds = MockServer.new(opts)
      @server = Thin::Server.new("localhost", PORT, sds)
    end

    def start
      Thread.new{ @server.start }
    end

    def stop
      @server.stop
    end

    def send_store(service, service_id, ori_path)
      parameter = {
                    "_method" => "put",
                    :service => service,
                    :service_id => service_id,
                    :data_file_path => ori_path,
                  }
      http = EM::HttpRequest.new("http://localhost:#{PORT}/serialized/#{service}/#{service_id}/serialized/data").put(:query => parameter)
      http.callback do
        @response_url = http.response
        @response = http.response_header.status
      end
      http.errback do
        @response = -1
      end
    end

    def get_file(service, service_id, token)
      parameter = {
                    :service => service,
                    :service_id => service_id,
                    :token => token,
                  }
      http = EM::HttpRequest.new("http://localhost:#{PORT}/serialized/#{service}/#{service_id}/serialized/file").get(:query => parameter)
      http.callback do
        @response = http.response_header.status
      end
      http.errback do
        @response = -1
      end
    end

    def delete_file(service, service_id, token)
      parameter = {
                    :service => service,
                    :service_id => service_id,
                    :token => token,
                  }
      http = EM::HttpRequest.new("http://localhost:#{PORT}/serialized/#{service}/#{service_id}/serialized/file").delete(:query => parameter)
      http.callback do
        @response = http.response_header.status
      end
      http.errback do
        @response = -1
      end
    end

    def get_snapshot(service, service_id, snapshot_id, token)
      parameter = {
                    :service => service,
                    :service_id => service_id,
                    :snapshot_id => snapshot_id,
                    :token => token,
                  }
      http = EM::HttpRequest.new("http://localhost:#{PORT}/serialized/#{service}/#{service_id}/snapshots/#{snapshot_id}").get(:query => parameter)
      http.callback do
        @response = http.response_header.status
      end
      http.errback do
        @response = -1
      end
    end
  end

  class MockServer < VCAP::Services::Serialization::Server
    def initialize(opts)
      super opts
      @store = MockStore.new(opts)
    end

    def authorized?
      true
    end
  end

  class MockStore < VCAP::Services::Serialization::Store
    def get_snapshot_file_path(service, service_id, snapshot_id, token)
      real_path = snapshot_file_path(service, service_id, snapshot_id, token)
      [200, real_path, token]
    end
  end
end
