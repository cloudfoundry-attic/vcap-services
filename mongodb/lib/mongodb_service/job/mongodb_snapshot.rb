# Copyright (c) 2009-2011 VMware, Inc.
require "mongodb_service/job/util"

module VCAP::Services::Snapshot::MongoDB
  include VCAP::Services::Snapshot

  class CreateSnapshotJob < SnapshotJob
    include VCAP::Services::MongoDB::Util
    def perform
      name = options['service_id']
      @logger.info("Begin create snapshot jobs for #{name}")
      VCAP::Services::Snapshot.redis_connect(@config["resque"])

      snapshot_id = get_snapshot_id
      dump_path = get_dump_path(name, snapshot_id)
      FileUtils.mkdir_p(dump_path)
      dump_file_name = File.join(dump_path, "#{snapshot_id}.tgz")

      result = dump_database(name, dump_file_name)
      raise "Failed to execute dump command to #{name}" unless result

      dump_file_size = File.size(dump_file_name)
      complete_time = Time.now
      snapshot = {
        :snapshot_id => snapshot_id,
        :date => complete_time.to_s,
        :size => dump_file_size
      }
      save_snapshot(name, snapshot)

      job_result = { :snapshot_id => snapshot_id }
      set_status({ :complete_time => complete_time.to_s })
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

  class RollbackSnapshotJob < SnapshotJob
    include VCAP::Services::MongoDB::Util
    def perform
      name = options['service_id']
      snapshot_id = options['snapshot_id']
      @logger.info("Begin rollback snapshot #{snapshot_id} job for #{name}")

      dump_path = get_dump_path(name, snapshot_id)
      dump_file_path = File.join(dump_path, "#{snapshot_id}.tgz")
      raise "Snapshot file #{dump_file_path} doesn't exist" unless File.exists?(dump_file_path)

      result = restore_database(name, dump_file_path)
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

