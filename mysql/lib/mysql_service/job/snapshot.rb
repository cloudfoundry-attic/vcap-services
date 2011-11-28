# Copyright (c) 2009-2011 VMware, Inc.
require "resque/job_with_status"
require "fileutils"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..', '..', 'base', 'lib')
require "base/service_error"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..')
require "util"
require "mysql_error"

module VCAP
  module Services
    module Snapshot
    end
  end
end

module VCAP::Services::Snapshot::Mysql
  extend self

  KEY_PREFIX = "vcap:snapshot:mysql".freeze

  def redis=(redis)
    raise "Snapshot requires redis configuration." unless redis
    @redis = redis
  end

  def service_snapshots(service_id)
    res = @redis.hgetall(redis_key(service_id))
    res.values.map{|v| Yajl::Parser.parse(v)}
  end

  def get_snapshot(service_id, snapshot_id)
    res = @redis.hget(redis_key(service_id), snapshot_id)
    Yajl::Parser.parse(res) if res
  end

  def redis_key(key)
    "#{KEY_PREFIX}:#{key}"
  end

  def logger=(logger)
    @logger = logger
    CreateSnapshotJob.logger = @logger
    RollbackSnapshotJob.logger = @logger
  end

  # common utils
  class SnapshotJob < Resque::JobWithStatus
    include VCAP::Services::Snapshot::Mysql
    include VCAP::Services::Base::Error
    include VCAP::Services::Mysql::Util

    def self.logger=(logger)
      @logger = logger
    end

    def self.queue_lookup_key
      :node_id
    end

    def self.select_queue(*args)
      result = nil
      args.each do |arg|
        result = arg[queue_lookup_key]if (arg.is_a? Hash )&& (arg.has_key?(queue_lookup_key))
      end
      @logger.info("Select queue #{result} for #{self} with args:#{args.inspect}")
      result
    end

    def make_logger
      return @logger if @logger
      @logger = Logger.new( STDOUT)
      @logger.level = Logger::DEBUG
      @logger
    end

    # the snapshot path structure looks like <base-dir>\snapshots\<service-name>\<aa>\<bb>\<cc>\
    # <aabbcc-rest-of-instance-guid>\snapshot_id\<service specific data>
    def get_dump_path(name, snapshot_id)
      File.join(@config["snapshots_base_dir"], "snapshots", @config["service_name"] , name[0,2],name[2,2], name[4,2], name, snapshot_id.to_s)
    end

    protected
    def parse_config
      @config = Yajl::Parser.parse(ENV['WORKER_CONFIG'])
      raise "Need environment variable: WORKER_CONFIG" unless @config
    end

    def redis_connect
      resque = %w(host port password).inject({}){|res, o| res[o.to_sym] = @config["resque"][o]; res}
      @redis = Redis.new(resque)
      redis_init
    end

    #initialze necessary keys
    def redis_init
      @redis.setnx(redis_key("maxid"), 1)
    end

    # Generate unique id for this snapshot
    def get_snapshot_id(name)
      @redis.incr(redis_key("maxid")).to_s
    end

    def save_snapshot(name, snapshot)
      msg = Yajl::Encoder.encode(snapshot)
      @redis.hset(redis_key(name), snapshot[:snapshot_id], msg)
    end

    def cleanup(name, snapshot_id)
      return unless name && snapshot_id
      @redis.hdel(redis_key(name), snapshot_id)
      FileUtils.rm_rf(get_dump_path(name, snapshot_id))
    end
  end

  # Dump a database into files and save the snapshot information into redis.
  class CreateSnapshotJob < SnapshotJob

    def perform
      name = options["service_id"]
      make_logger
      @logger.info("Begin create snapshot job for #{name}")
      parse_config
      redis_connect

      snapshot_id = get_snapshot_id(name)
      dump_path = get_dump_path(name, snapshot_id)
      FileUtils.mkdir_p(dump_path)
      dump_file_name = File.join(dump_path, "#{snapshot_id}.sql.gz")

      mysql_conf = @config["mysql"]
      result = dump_database(name, mysql_conf, dump_file_name, :mysqldump_bin => @config["mysqldump_bin"], :gzip_bin => @config["gzip_bin"])
      raise "Failed to execute dump command to #{name}" unless result

      dump_file_size = -1
      File.open(dump_file_name) {|f| dump_file_size = f.size}
      complete_time = Time.now
      snapshot = {
        :snapshot_id => snapshot_id,
        :date => complete_time.to_s,
        :size => dump_file_size
      }
      save_snapshot(name, snapshot)

      job_result = { :snapshot_id => snapshot_id }
      set_status({:complete_time => complete_time.to_s})
      completed(Yajl::Encoder.encode(job_result))
    rescue => e
      @logger.error("Error in CreateSnapshotJob #{@uuid}:#{fmt_error(e)}")
      cleanup(name, snapshot_id)
      err = (e.instance_of?(ServiceError)? e : ServiceError.new(ServiceError::INTERNAL_ERROR)).to_hash
      err_msg = Yajl::Encoder.encode(err)
      set_status({:complete_time => Time.now.to_s})
      failed(err_msg)
    end
  end

  # Rollback data from snapshot files.
  class RollbackSnapshotJob < SnapshotJob

    def perform
      make_logger
      name = options["service_id"]
      snapshot_id = options["snapshot_id"]
      @logger.info("Begin rollback snapshot #{snapshot_id} job for #{name}")
      parse_config

      mysql_conf = @config["mysql"]
      snapshot_file_path = File.join(get_dump_path(name, snapshot_id) , "#{snapshot_id}.sql.gz")
      raise "Can't snapshot file #{snapshot_file_path}" unless File.exists?(snapshot_file_path)

      result = import_dumpfile(name, mysql_conf, snapshot_file_path, :mysql_bin => @config["mysql_bin"], :gzip_bin => @config["gzip_bin"])
      raise "Failed execute import command to #{name}" unless result

      set_status({:complete_time => Time.now.to_s})
      completed(Yajl::Encoder.encode({:result => "ok"}))
    rescue => e
      @logger.error("Error in Rollback snapshot job #{@uuid}:#{fmt_error(e)}")
      err = (e.instance_of?(ServiceError)? e : ServiceError.new(ServiceError::INTERNAL_ERROR)).to_hash
      err_msg = Yajl::Encoder.encode(err)
      set_status({:complete_time => Time.now.to_s})
      failed(err_msg)
    end
  end
end
