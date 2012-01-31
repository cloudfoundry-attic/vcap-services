# Copyright (c) 2009-2011 VMware, Inc.

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..')
require "util"
require "mysql_error"

module VCAP::Services::Snapshot::Mysql
  include VCAP::Services::Snapshot

  # Dump a database into files and save the snapshot information into redis.
  class CreateSnapshotJob < SnapshotJob
    include VCAP::Services::Mysql::Util

    def perform
      name = options["service_id"]
      @logger.info("Begin create snapshot job for: #{name}")
      VCAP::Services::Snapshot.redis_connect(@config["resque"])

      snapshot_id = get_snapshot_id
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
    include VCAP::Services::Mysql::Util

    def perform
      name = options["service_id"]
      snapshot_id = options["snapshot_id"]
      @logger.info("Begin rollback snapshot #{snapshot_id} job for #{name}")

      mysql_conf = @config["mysql"]
      snapshot_file_path = File.join(get_dump_path(name, snapshot_id) , "#{snapshot_id}.sql.gz")
      raise "Can't snapshot file #{snapshot_file_path}" unless File.exists?(snapshot_file_path)

      result = import_dumpfile(name, mysql_conf, mysql_conf["user"], mysql_conf["pass"], snapshot_file_path, :mysql_bin => @config["mysql_bin"], :gzip_bin => @config["gzip_bin"])
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
