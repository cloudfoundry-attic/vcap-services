# Copyright (c) 2009-2011 VMware, Inc.
require "resque/job_with_status"
require "fileutils"
require "vcap/logging"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..')
require "service_error"

module VCAP
  module Services
  end
end

module VCAP::Services::Snapshot

  SNAPSHOT_KEY_PREFIX = "vcap:snapshot".freeze
  SNAPSHOT_ID = "maxid".freeze

  class << self
    attr_reader :redis, :logger

    # Config the redis using options
    def redis_connect(opts)
      resque = %w(host port password).inject({}){|res, o| res[o.to_sym] = opts[o]; res}
      @redis = ::Redis.new(resque)

      redis_init
    end

    # Supply a redis instance
    def redis=(redis)
      raise "Snapshot requires redis configuration." unless redis
      @redis = redis

      redis_init
    end

    # initialize necessary keys
    def redis_init
      @redis.setnx("#{SNAPSHOT_KEY_PREFIX}:#{SNAPSHOT_ID}", 1)
    end

    def logger=(logger)
      @logger = logger
    end
  end

  def client
    VCAP::Services::Snapshot.redis
  end

  # Get all snapshots related to a service instance
  #
  def service_snapshots(service_id)
    return unless service_id
    res = client.hgetall(redis_key(service_id))
    res.values.map{|v| Yajl::Parser.parse(v)}
  end

  # Get detail information for a single snapshot
  #
  def snapshot_details(service_id, snapshot_id)
    return unless service_id && snapshot_id
    res = client.hget(redis_key(service_id), snapshot_id)
    Yajl::Parser.parse(res) if res
  end

  # Generate unique id for a snapshot
  def get_snapshot_id
    client.incr(redis_key(SNAPSHOT_ID)).to_s
  end

  def save_snapshot(service_id , snapshot)
    return unless service_id && snapshot
    msg = Yajl::Encoder.encode(snapshot)
    client.hset(redis_key(service_id), snapshot[:snapshot_id], msg)
  end

  def delete_snapshot(service_id , snapshot_id)
    return unless service_id && snapshot_id
    client.hdel(redis_key(name), snapshot_id)
  end

  protected

  def redis_key(key)
    "#{SNAPSHOT_KEY_PREFIX}:#{key}"
  end

  # common utils for snapshot job
  class SnapshotJob < Resque::JobWithStatus
    include VCAP::Services::Snapshot
    include VCAP::Services::Base::Error

    class << self
      attr_reader :logger

      def queue_lookup_key
        :node_id
      end

      def logger=(logger)
        @logger = logger
      end

      def select_queue(*args)
        result = nil
        args.each do |arg|
          result = arg[queue_lookup_key]if (arg.is_a? Hash)&& (arg.has_key?(queue_lookup_key))
        end
        @logger.info("Select queue #{result} for job #{self.class} with args:#{args.inspect}") if @logger
        result
      end
    end

    def initialize(*args)
      super(*args)
      parse_config
      init_worker_logger()
    end

    def init_worker_logger
      VCAP::Logging.setup_from_config(@config["logging"])
      @logger = VCAP::Logging.logger("#{@config["service_name"]}_worker")
    end

    # the snapshot path structure looks like <base-dir>\snapshots\<service-name>\<aa>\<bb>\<cc>\
    # <aabbcc-rest-of-instance-guid>\snapshot_id\<service specific data>
    def get_dump_path(name, snapshot_id)
      File.join(@config["snapshots_base_dir"], "snapshots", @config["service_name"] , name[0,2],name[2,2], name[4,2], name, snapshot_id.to_s)
    end

    def parse_config
      @config = Yajl::Parser.parse(ENV['WORKER_CONFIG'])
      raise "Need environment variable: WORKER_CONFIG" unless @config
    end

    def cleanup(name, snapshot_id)
      return unless name && snapshot_id
      delete_snapshot(name, snapshot_id)
      FileUtils.rm_rf(get_dump_path(name, snapshot_id))
    end
  end
end
