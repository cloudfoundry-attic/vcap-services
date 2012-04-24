# Copyright (c) 2009-2011 VMware, Inc.
require "mongodb_service/job/util"

module VCAP::Services::MongoDB::Snapshot
  include VCAP::Services::Base::AsyncJob::Snapshot

  class CreateSnapshotJob < BaseCreateSnapshotJob
    include VCAP::Services::MongoDB::Util

    def execute
      dump_path = get_dump_path(name, snapshot_id)
      FileUtils.mkdir_p(dump_path)
      dump_file_name = File.join(dump_path, "#{snapshot_id}.tgz")

      result = dump_database(name, dump_file_name)
      raise "Failed to execute dump command to #{name}" unless result

      dump_file_size = File.size(dump_file_name)
      complete_time = Time.now
      snapshot = {
        :snapshot_id => snapshot_id,
        :size => dump_file_size
      }

      snapshot
    end
  end

  class RollbackSnapshotJob < BaseRollbackSnapshotJob
    include VCAP::Services::MongoDB::Util

    def execute
      dump_path = get_dump_path(name, snapshot_id)
      dump_file_path = File.join(dump_path, "#{snapshot_id}.tgz")
      raise "Snapshot file #{dump_file_path} doesn't exist" unless File.exists?(dump_file_path)

      result = restore_database(name, dump_file_path)
      raise "Failed execute import command to #{name}" unless result

      true
    end
  end
end

