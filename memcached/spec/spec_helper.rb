# Copyright (c) 2009-2011 VMware, Inc.
# This code is based on Redis as a Service.

$:.unshift File.join(File.dirname(__FILE__), '..')
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

ENV["BUNDLE_GEMFILE"] ||= File.expand_path("../../Gemfile", __FILE__)

require 'rubygems'
require 'rspec'
require 'bundler/setup'
require 'nats/client'
require 'vcap/common'
require "datamapper"
require "uri"
require "thread"


def get_hostname(credentials)
  host = credentials['host']
  port = credentials['port'].to_s
  hostname = host + ":" + port
  return hostname
end

def get_connect_info(credentials)
  hostname = get_hostname(credentials)
  username = @credentials['user']
  password = @credentials['password']

  return [hostname, username, password]
end
