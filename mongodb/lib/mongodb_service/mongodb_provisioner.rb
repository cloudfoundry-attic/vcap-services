# Copyright (c) 2009-2011 VMware, Inc.
require "mongodb_service/common"
require "mongodb_service/job/util"
require "mongodb_service/job/mongodb_snapshot"
require "mongodb_service/job/mongodb_serialization"

class VCAP::Services::MongoDB::Provisioner < VCAP::Services::Base::Provisioner

  include VCAP::Services::MongoDB::Common

  def initialize(opts)
    super(opts)
    @opts = opts
    VCAP::Services::Snapshot.logger = @logger
    VCAP::Services::Serialization.logger = @logger
    VCAP::Services::AsyncJob.logger = @logger
  end

  def pre_send_announcement
    addition_opts = @opts[:additional_options]
    if addition_opts
      @upload_temp_dir = addition_opts[:upload_temp_dir]
      if addition_opts[:resque]
        resque_opt = addition_opts[:resque]
        redis = create_redis(resque_opt)
        job_repo_setup(:redis => redis)
        VCAP::Services::Snapshot.redis = redis
      end
    end
  end

  def create_snapshot_job
    VCAP::Services::Snapshot::MongoDB::CreateSnapshotJob
  end

  def rollback_snapshot_job
    VCAP::Services::Snapshot::MongoDB::RollbackSnapshotJob
  end

  def create_serialized_url_job
    VCAP::Services::Serialization::MongoDB::CreateSerializedURLJob
  end

  def import_from_url_job
    VCAP::Services::Serialization::MongoDB::ImportFromURLJob
  end

  def import_from_data_job
    VCAP::Services::Serialization::MongoDB::ImportFromDataJob
  end

end

