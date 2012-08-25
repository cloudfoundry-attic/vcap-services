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
#require "webmock/rspec"
require "bundler/setup"
#require "vcap_services_base"
require "rack/test"
require "json"
require "logger"
require "yaml"
#require "webmock"


#include WebMock::API

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "..", "..")
#require "vcap/common"

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

def stub_fixture(verb, api, path, scenario = "")
  url = "#{api}/#{path}"
  fixture = "#{scenario}#{path}/#{verb.to_s}_response.json"
  req_fixture = "#{scenario}#{path}/#{verb.to_s}_request.json"
  stuff = load_fixture(fixture)
  #stub_request(verb, url).to_return(:body=> stuff)
  JSON.parse(load_fixture(req_fixture))
end

def stub_cc_request(verb, path, scenario = "")
  stuff = load_fixture("#{scenario}cloudfoundry/#{path}/#{verb.to_s}_response.json", '')

  i = 0
  while File.exists?("#{File.dirname(__FILE__)}/fixtures/#{scenario}cloudfoundry/#{path}/#{verb.to_s}_request_#{i}.json")
    req_path = "#{scenario}cloudfoundry/#{path}/#{verb.to_s}_request_#{i}.json"
    req_body =  JSON.parse(load_fixture(req_path))
    #stub_http_request(:post, "http://api.vcap.me/#{path}/")
    #  .with(:body=> symbolize_keys(req_body), :headers => {'Content-Type'=>'application/json', 'X-Vcap-Service-Token'=> /.+/})
    #  .to_return(:status => 200, :body => "", :headers => {})
    i += 1
  end
end

def load_config()
  config = YAML.load_file(File.join(File.dirname(__FILE__), "..", "..", "config", "marketplace_gateway.yml"))
  config = symbolize_keys(config)
  appdirect_config = YAML.load_file(File.join(File.dirname(__FILE__), "..", "..", "config", "appdirect.yml"))
  appdirect_config = symbolize_keys(appdirect_config)

  config = config.merge(appdirect_config)
  config[:logger] = make_logger()
  config[:host] = VCAP.local_ip(config[:ip_route])
  config[:port] ||= VCAP.grab_ephemeral_port
  config[:url] = "http://#{config[:host]}:#{config[:port]}"

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

# http://eigenclass.org/hiki/Changes+in+Ruby+1.9#l156
# Default Time.to_s changed in 1.9, monkeypatching it back
class Time
  def to_s
    strftime("%a %b %d %H:%M:%S %Z %Y")
  end
end
