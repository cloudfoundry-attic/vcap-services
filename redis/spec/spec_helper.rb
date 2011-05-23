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

def test_exception(exception_type)
  thrown = nil
  begin
    yield
  rescue => e
    thrown = e
  end
  thrown.should be
  thrown.class.should == exception_type
end
