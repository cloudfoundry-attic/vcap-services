# Copyright (c) 2009-2011 VMware, Inc.
require "eventmachine"
require "thread"

module VCAP
  module Services
    module Base
    end
  end
end

class VCAP::Services::Base::Barrier

  def initialize(options = {}, &callback)
    raise ArgumentError unless options[:timeout] || options[:callbacks]
    @lock = Mutex.new
    @callback = callback
    @expected_callbacks = options[:callbacks]
    @timer = EM.add_timer(options[:timeout]) {on_timeout} if options[:timeout]
    @callback_fired = false
    @responses = []
    @barrier_callback = Proc.new {|*args| call(*args)}
  end

  def on_timeout
    @lock.synchronize do
      unless @callback_fired
        @callback_fired = true
        @callback.call(@responses)
      end
    end
  end

  def call(*args)
    @lock.synchronize do
      unless @callback_fired
        @responses << args
        if @expected_callbacks
          @expected_callbacks -= 1
          if @expected_callbacks <= 0
            EM.cancel_timer(@timer) if @timer
            @callback_fired = true
            @callback.call(@responses)
          end
        end
      end
    end
  end

  def callback
    @barrier_callback
  end

end
