# Copyright (c) 2009-2011 VMware, Inc.

require 'fileutils'
require 'redis'
require 'base64'

require 'base/provisioner'
require 'base/job/async_job'

require 'postgresql_service/common'
require 'postgresql_service/job'
require 'postgresql_service/job/postgresql_snapshot'
require 'postgresql_service/job/postgresql_serialization'

class VCAP::Services::Postgresql::Provisioner < VCAP::Services::Base::Provisioner

  include VCAP::Services::Postgresql::Common

  def initialize(opts)
    super(opts)
    @opts = opts
    VCAP::Services::Postgresql::Job::setup_job_logger(@logger)
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
    VCAP::Services::Snapshot::Postgresql::CreateSnapshotJob
  end

  def rollback_snapshot_job
    VCAP::Services::Snapshot::Postgresql::RollbackSnapshotJob
  end

  def create_serialized_url_job
    VCAP::Services::Serialization::Postgresql::CreateSerializedURLJob
  end

  def import_from_url_job
    VCAP::Services::Serialization::Postgresql::ImportFromURLJob
  end

  def import_from_data_job
    VCAP::Services::Serialization::Postgresql::ImportFromDataJob
  end

end
