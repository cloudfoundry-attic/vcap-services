# Copyright (c) 2009-2011 VMware, Inc.
require "fileutils"
require "redis"
require "base64"
require "base/provisioner"
require "base/job/async_job"

$LOAD_PATH.unshift File.join(File.dirname __FILE__)
require "common"
require "job/redis_snapshot"
require "job/redis_serialization"

class VCAP::Services::Redis::Provisioner < VCAP::Services::Base::Provisioner

  include VCAP::Services::Redis::Common

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
    VCAP::Services::Snapshot::Redis::CreateSnapshotJob
  end

  def rollback_snapshot_job
    VCAP::Services::Snapshot::Redis::RollbackSnapshotJob
  end

  def create_serialized_url_job
    VCAP::Services::Serialization::Redis::CreateSerializedURLJob
  end

  def import_from_url_job
    VCAP::Services::Serialization::Redis::ImportFromURLJob
  end

  def import_from_data_job
    VCAP::Services::Serialization::Redis::ImportFromDataJob
  end

end
