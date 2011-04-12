# Copyright (c) 2009-2011 VMware, Inc.
require 'spec_helper'

describe MysqlTests do
  it "should have tests" do
    mysql_tests = MysqlTests.new
    mysql_tests.has_tests.should be_true
  end
end
