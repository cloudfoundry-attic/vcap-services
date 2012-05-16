# Copyright (c) 2009-2012 VMware, Inc.
require "mysql2"
require "timeout"

module Mysql2
  class Client
    class << self
      attr_accessor :default_timeout
      attr_accessor :logger
    end

    alias :origin_initialize :initialize
    alias :origin_query :query

    def initialize(opts={})
      client = origin_initialize(opts)
      wait_timeout = self.class.default_timeout
      client.query("set @@wait_timeout=#{wait_timeout}") if wait_timeout
    end

    def query(query_str)
      wait_timeout = self.class.default_timeout
      if wait_timeout
        begin
          Timeout::timeout(wait_timeout) { return origin_query query_str }
        rescue Timeout::Error => e
          self.class.logger.error("Mysql query timeout after running for #{wait_timeout} seconds: [#{query_str}]") if self.class.logger
          raise e
        end
      else
        return origin_query query_str
      end
    end

  end
end
