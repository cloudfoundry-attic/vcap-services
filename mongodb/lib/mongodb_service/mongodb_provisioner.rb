# Copyright (c) 2009-2011 VMware, Inc.
require "mongodb_service/common"
require "mongodb_service/job/util"
require "mongodb_service/job"

class VCAP::Services::MongoDB::Provisioner < VCAP::Services::Base::Provisioner
  include VCAP::Services::MongoDB::Common

  def create_snapshot_job
    VCAP::Services::MongoDB::Snapshot::CreateSnapshotJob
  end

  def rollback_snapshot_job
    VCAP::Services::MongoDB::Snapshot::RollbackSnapshotJob
  end

  def delete_snapshot_job
    VCAP::Services::Base::AsyncJob::Snapshot::BaseDeleteSnapshotJob
  end

  def create_serialized_url_job
    VCAP::Services::MongoDB::Serialization::CreateSerializedURLJob
  end

  def import_from_url_job
    VCAP::Services::MongoDB::Serialization::ImportFromURLJob
  end

  def import_from_data_job
    VCAP::Services::MongoDB::Serialization::ImportFromDataJob
  end

end

