# Copyright (c) 2009-2011 VMware, Inc.
require "mongodb_service/job/util"

module VCAP::Services::MongoDB::Snapshot
  include VCAP::Services::Base::AsyncJob::Snapshot

  class CreateSnapshotJob < BaseCreateSnapshotJob
    include VCAP::Services::MongoDB::Util

    def execute
      dump_path = get_dump_path(name, snapshot_id)
      FileUtils.mkdir_p(dump_path)
      filename = "#{snapshot_id}.tgz"
      dump_file_name = File.join(dump_path, filename)

      setup_localdb
      version = instance_version(name)
      result = dump_database(name, dump_file_name)
      raise "Failed to execute dump command to #{name}" unless result

      dump_file_size = File.size(dump_file_name)
      complete_time = Time.now
      snapshot = {
        :snapshot_id => snapshot_id,
        :size => dump_file_size,
        :files => [filename],
        :manifest => {
          :version => 1,
          :service_version => version
        }
      }

      snapshot
    end
  end

  class RollbackSnapshotJob < BaseRollbackSnapshotJob
    include VCAP::Services::MongoDB::Util

    def execute
      dump_file_path = @snapshot_files[0]
      raise "Snapshot file #{dump_file_path} doesn't exist" unless File.exists?(dump_file_path)

      setup_localdb
      result = restore_database(name, dump_file_path)
      raise "Failed execute import command to #{name}" unless result

      true
    end
  end
end

