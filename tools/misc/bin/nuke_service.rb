#!/usr/bin/env ruby
# Copyright (c) 2009-2011 VMware, Inc.
require 'rubygems'

require 'json'
require 'logger'
require 'net/http'
require 'optparse'
require 'set'
require 'uri'
require 'yaml'

def symbolize_keys(hash)
  if hash.is_a? Hash
    new_hash = {}
    hash.each {|k, v| new_hash[k.to_sym] = symbolize_keys(v) }
    new_hash
  else
    hash
  end
end

config_file  = nil
service      = nil
label        = nil
token        = nil
scripted     = false
cld_ctrl_uri = nil

default_configs = {
  :mongodb => File.expand_path("../../mongodb/config/mongodb_gateway.yml", __FILE__),
  :redis   => File.expand_path("../../redis/config/redis_gateway.yml", __FILE__),
  :mysql   => File.expand_path("../../mysql/config/mysql_gateway.yml", __FILE__),
  :neo4j   => File.expand_path("../../neo4j/config/neo4j_gateway.yml", __FILE__),
  :vblob   => File.expand_path("../../vblob/config/vblob_gateway.yml", __FILE__),
  :echo    => File.expand_path("../../echo/config/echo_gateway.yml", __FILE__),
}

OptionParser.new do |opts|
  opts.banner = "Usage: nuke_service [options]"

  opts.on("-c", "--config ARG", "Config file") do |opt|
    config_file = opt
  end

  opts.on("-C", "--cloud-controller ARG", "Cloud controller uri") do |opt|
    cld_ctrl_uri = opt
  end

  opts.on("-s", "--service ARG", [:redis, :mysql, :mongodb, :neo4j],  "Look for default configs for the supplied service [redis, mysql, mongodb, neo4j]") do |opt|
    config_file = default_configs[opt]
  end

  opts.on("-l", "--label ARG", "Service label") do |opt|
    label = opt
  end

  opts.on("-t", "--token ARG", "Service token") do |opt|
    token = opt
  end

  opts.on("-S", "--scripted", "Don't prompt before taking action, silence output") do |opt|
    scripted = true
  end

  opts.on("-h", "--help", "Help") do
    puts opts
    exit 1
  end
end.parse!

logger = Logger.new(STDOUT)
logger.level = (scripted ? Logger::FATAL : Logger::DEBUG)

if config_file
  begin
    config = symbolize_keys(YAML.load_file(config_file))
  rescue => e
    logger.error("Failed loading config: #{e}")
    exit 1
  end

  label ||= "#{config[:service][:name]}-#{config[:service][:version]}"
  token ||= config[:token]
  cld_ctrl_uri ||= "http://#{config[:cloud_controller][:host]}:#{config[:cloud_controller][:port]}"
end

missing = {}
missing[:label] = "Missing label" unless label
missing[:token] = "Missing token" unless token
missing[:uri] = "Missing cloud controller uri" unless cld_ctrl_uri
if !missing.empty?
  logger.error(missing.values.join(", "))
  exit 1
end

unless scripted
  logger.info("You are about to destroy all configs and bindings for the following:")
  logger.info("Service: #{label}")
  logger.info("Token:   #{token}")
  logger.info("URI:     #{cld_ctrl_uri}")
  answer = nil
  allowed_answers = Set.new(['y', 'n'])
  while !allowed_answers.include?(answer)
    logger.info("Are you sure you want to continue? (y/n) ")
    answer = gets.strip
  end
  if answer == 'n'
    logger.info("You got it, aborting!")
    exit 0
  end
end

success = false
begin
  hdr = {
    'Content-Type' => 'application/json',
    'X-VCAP-Service-Token' => token
  }
  uri = URI.parse(cld_ctrl_uri)
  req = Net::HTTP::Delete.new("/services/v1/offerings/#{label}", initheader = hdr)
  resp = Net::HTTP.new(uri.host, uri.port).start {|http| http.request(req)}
  case resp
  when Net::HTTPSuccess
    logger.info("Success")
    success = true

  when Net::HTTPBadRequest
    errs = JSON.parse(resp.body)
    logger.error("Failure!: #{errs}")

  else
    logger.error("Failure!")
    logger.error("Unexpected response:")
    logger.error("Status: #{resp.code}")
    logger.error("Body: #{resp.body}")
  end
rescue => e
  logger.error("Failure talking to cloud controller: #{e}")
end

exit (success ? 0 : 1)
