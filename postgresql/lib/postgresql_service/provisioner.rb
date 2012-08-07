# Copyright (c) 2009-2011 VMware, Inc.

require 'fileutils'
require 'redis'
require 'base64'

require 'postgresql_service/common'
require 'postgresql_service/job'

class VCAP::Services::Postgresql::Provisioner < VCAP::Services::Base::Provisioner
  include VCAP::Services::Postgresql::Common

  def create_snapshot_job
    VCAP::Services::Postgresql::Snapshot::CreateSnapshotJob
  end

  def rollback_snapshot_job
    VCAP::Services::Postgresql::Snapshot::RollbackSnapshotJob
  end

  def delete_snapshot_job
    VCAP::Services::Base::AsyncJob::Snapshot::BaseDeleteSnapshotJob
  end

  def create_serialized_url_job
    VCAP::Services::Base::AsyncJob::Serialization::BaseCreateSerializedURLJob
  end

  def import_from_url_job
    VCAP::Services::Postgresql::Serialization::ImportFromURLJob
  end

end
