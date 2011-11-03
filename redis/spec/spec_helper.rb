# Copyright (c) 2009-2011 VMware, Inc.
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
require "redis"
require "thread"
