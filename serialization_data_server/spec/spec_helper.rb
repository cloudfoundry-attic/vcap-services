# Copyright (c) 2009-2011 VMware, Inc.

require 'rubygems'
require 'rspec'

$:.unshift File.join(File.dirname(__FILE__), '..')
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')
require 'server'

def getLogger()
  logger = Logger.new( STDOUT)
  logger.level = Logger::ERROR
  return logger
end
