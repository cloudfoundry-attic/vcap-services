# Copyright (c) 2009-2012 VMware, Inc.
require 'helper/job_spec_helper'

describe VCAP::Services::Base::AsyncJob::Lock do

  before :all do
    @timeout = 10
    @expiration = 5
    @logger = Logger.new(STDOUT)
    @logger.level = Logger::ERROR
    @name = "foo"
  end

  before :each do
    @redis = mock("redis")

    @stored_value = nil
    Redis.should_receive(:new).any_number_of_times.and_return(@redis)
    VCAP::Services::Base::AsyncJob::Config.redis_config = {}

    @redis.should_receive(:setnx).with(@name, anything).any_number_of_times.and_return do |_, value|
      if @stored_value.nil?
        @stored_value = value
        true
      else
        nil
      end
    end
    @redis.should_receive(:get).with(@name).any_number_of_times.and_return do
      @stored_value
    end
    @redis.should_receive(:set).with(@name, anything).any_number_of_times.and_return do |_, value|
      @stored_value = value
    end
    @redis.should_receive(:del).with(@name).and_return do
      @stored_value = nil
      nil
    end
  end

  it "should acquire and release a lock" do
    @redis.should_receive(:watch).with(@name).any_number_of_times
    @redis.should_receive(:multi).any_number_of_times.and_yield

    lock = VCAP::Services::Base::AsyncJob::Lock.new(@name,:timeout => @timeout, :expiration => @expiration)

    ran_once = false
    lock.lock do
      ran_once = true
    end

    ran_once.should be_true
  end

  it "should not let two clients acquire the same lock at the same time" do
    @redis.should_receive(:watch).with(@name).any_number_of_times
    @redis.should_receive(:multi).any_number_of_times.and_yield

    lock_a = VCAP::Services::Base::AsyncJob::Lock.new(@name,:timeout => @timeout, :expiration => @expiration, :logger => @logger)
    lock_b = VCAP::Services::Base::AsyncJob::Lock.new(@name,:timeout => 0.1, :logger => @logger)

    lock_a_ran = false
    lock_b_ran = false
    lock_a.lock do
      lock_a_ran = true
      expect{lock_b.lock{ lock_b_ran = true } }.should raise_error(VCAP::Services::Base::Error::ServiceError, /Job timeout/)
    end

    lock_a_ran.should be_true
    lock_b_ran.should_not be_true
  end

  it "should acquire an expired lock" do
    start = Time.now.to_f
    @stored_value = (start + 3)  #lock that expires in 3 seconds

    @redis.should_receive(:watch).with(@name).any_number_of_times
    @redis.should_receive(:multi).any_number_of_times.and_yield

    lock = VCAP::Services::Base::AsyncJob::Lock.new(@name,:timeout => @timeout, :expiration => @expiration, :logger => @logger)

    ran_once = false
    lock.lock{ran_once = true}

    ran_once.should be_true
    (@stored_value == start - 1).should_not be_true
  end

  it "should not update expiration time after the lock is released" do
    start = Time.now.to_f

    @redis.should_receive(:watch).with(@name).any_number_of_times
    @redis.should_receive(:multi).any_number_of_times.and_yield

    expiration = 0.5
    lock = VCAP::Services::Base::AsyncJob::Lock.new(@name,:timeout => @timeout, :expiration => expiration, :logger => @logger)

    ran_once = false
    lock.lock{ran_once = true; sleep expiration *2 }

    current_value = @stored_value
    sleep expiration * 2
    current_value.should == @stored_value
  end
end
