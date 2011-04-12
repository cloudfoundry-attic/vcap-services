# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), '..')
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'rubygems'
require 'rspec'

class MysqlTests
  def initialize
    @has_tests = false
  end

  def has_tests
    @has_tests
  end
end

