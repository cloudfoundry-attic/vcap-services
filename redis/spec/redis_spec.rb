# Copyright (c) 2009-2011 VMware, Inc.
require 'spec_helper'

describe RedisTests do
  it "should have tests" do
    redis_tests = RedisTests.new
    redis_tests.has_tests.should be_true
  end
end
