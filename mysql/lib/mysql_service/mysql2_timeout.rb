# Copyright (c) 2009-2012 VMware, Inc.
require "mysql2"

module Mysql2
  class Client
    class << self
      attr_accessor :default_timeout
    end

    alias :origin_initialize :initialize

    def initialize(opts={})
      client = origin_initialize(opts)
      wait_timeout = self.class.default_timeout
      client.query("set @@wait_timeout=#{wait_timeout}") if wait_timeout
    end
  end
end
