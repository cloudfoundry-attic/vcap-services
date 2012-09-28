# Copyright (c) 2009-2011 VMware, Inc.
require "fileutils"
require "redis"
require "base64"

$LOAD_PATH.unshift File.join(File.dirname __FILE__)
require "common"
require "job"

class VCAP::Services::Redis::Provisioner < VCAP::Services::Base::Provisioner
  include VCAP::Services::Redis::Common

  def create_snapshot_job
    VCAP::Services::Redis::Snapshot::CreateSnapshotJob
  end

  def rollback_snapshot_job
    VCAP::Services::Redis::Snapshot::RollbackSnapshotJob
  end

  def delete_snapshot_job
    VCAP::Services::Base::AsyncJob::Snapshot::BaseDeleteSnapshotJob
  end

  def create_serialized_url_job
    VCAP::Services::Base::AsyncJob::Serialization::BaseCreateSerializedURLJob
  end

  def import_from_url_job
    VCAP::Services::Redis::Serialization::ImportFromURLJob
  end

end
