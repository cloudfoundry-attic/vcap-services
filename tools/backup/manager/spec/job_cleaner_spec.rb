# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require 'spec_helper'

describe JobCleanerTests do

  before :all do
    @options = {
      :max_days => JobCleanerTests::MAX_DAYS
    }
  end

  it "remove stale jobs" do
    JobCleanerTests.create_cleaner(@options) do |cleaner|
      cleaner.run
      cleaner.jobs.length.should == 2
    end
  end

end

