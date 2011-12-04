# Copyright (c) 2009-2011 VMware, Inc.
require 'fileutils'
require 'redis'
require 'base64'

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..', 'base', 'lib')

require 'base/provisioner'
require 'mysql_service/common'
require 'mysql_service/job/async_job'
require 'mysql_service/job/snapshot'
require 'mysql_service/job/serialization'

class VCAP::Services::Mysql::Provisioner < VCAP::Services::Base::Provisioner

  include VCAP::Services::Mysql::Common
  include VCAP::Services::Snapshot::Mysql
  include VCAP::Services::Serialization::Mysql
  include VCAP::Services::AsyncJob

  def initialize(opts)
    super(opts)
    @opts = opts
    VCAP::Services::Snapshot::Mysql.logger = @logger
    VCAP::Services::Serialization::Mysql.logger = @logger
    VCAP::Services::AsyncJob.logger = @logger
  end

  def pre_send_announcement
    if @opts[:additional_options]
      @upload_temp_dir = @opts[:additional_options][:upload_temp_dir]
      if @opts[:additional_options][:resque]
        resque_opt = @opts[:additional_options][:resque]
        redis = create_redis(resque_opt)
        expire = resque_opt[:expire]

        job_repo_setup(:redis => redis, :expire => expire)
        VCAP::Services::Snapshot::Mysql.redis = redis
      end
    end
  end

  def create_redis(opt)
    redis_client = Redis.new(opt)
    raise "Can't connect to redis:#{opt.inspect}" unless redis_client
    redis_client
  end

  def node_score(node)
    node['available_storage'] if node
  end

  def create_snapshot(service_id, &blk)
    @logger.debug("Create snapshot job for service_id=#{service_id}")
    svc = @prov_svcs[service_id]
    raise ServiceError.new(ServiceError::NOT_FOUND, service_id) unless svc
    job_id = CreateSnapshotJob.create(:service_id => service_id,
                  CreateSnapshotJob.queue_lookup_key =>find_node(service_id))
    job = get_job(job_id)
    @logger.info("CreateSnapshotJob created: #{job.inspect}")
    blk.call(success(job))
  rescue => e
    wrap_error(e, &blk)
  end

  def job_details(service_id, job_id, &blk)
    @logger.debug("Get job_id=#{job_id} for service_id=#{service_id}")
    svc = @prov_svcs[service_id]
    raise ServiceError.new(ServiceError::NOT_FOUND, service_id) unless svc
    job = get_job(job_id)
    raise ServiceError.new(ServiceError::NOT_FOUND, job_id) unless job
    blk.call(success(job))
  rescue => e
    wrap_error(e, &blk)
  end

  def get_snapshot(service_id, snapshot_id, &blk)
    @logger.debug("Get snapshot_id=#{snapshot_id} for service_id=#{service_id}")
    svc = @prov_svcs[service_id]
    raise ServiceError.new(ServiceError::NOT_FOUND, service_id) unless svc
    snapshot = VCAP::Services::Snapshot::Mysql.get_snapshot(service_id, snapshot_id)
    raise ServiceError.new(ServiceError::NOT_FOUND, snapshot_id) unless snapshot
    blk.call(success(snapshot))
  rescue => e
    wrap_error(e, &blk)
  end

  def enumerate_snapshots(service_id, &blk)
    @logger.debug("Get snapshots for service_id=#{service_id}")
    svc = @prov_svcs[service_id]
    raise ServiceError.new(ServiceError::NOT_FOUND, service_id) unless svc
    snapshots = VCAP::Services::Snapshot::Mysql.service_snapshots(service_id)
    blk.call(success({:snapshots => snapshots}))
  rescue => e
    wrap_error(e, &blk)
  end

  def rollback_snapshot(service_id, snapshot_id, &blk)
    @logger.debug("Rollback snapshot=#{snapshot_id} for service_id=#{service_id}")
    svc = @prov_svcs[service_id]
    raise ServiceError.new(ServiceError::NOT_FOUND, service_id) unless svc
    snapshot = VCAP::Services::Snapshot::Mysql.get_snapshot(service_id, snapshot_id)
    raise ServiceError.new(ServiceError::NOT_FOUND, snapshot_id) unless snapshot
    job_id = RollbackSnapshotJob.create(:service_id => service_id, :snapshot_id => snapshot_id,
                  RollbackSnapshotJob.queue_lookup_key => find_node(service_id))
    job = get_job(job_id)
    @logger.info("RoallbackSnapshotJob created: #{job.inspect}")
    blk.call(success(job))
  rescue => e
    wrap_error(e, &blk)
  end

  def get_serialized_url(service_id, &blk)
    @logger.debug("get serialized url for service_id=#{service_id}")
    svc = @prov_svcs[service_id]
    raise ServiceError.new(ServiceError::NOT_FOUND, service_id) unless svc
    job_id = CreateSerializedURLJob.create(:service_id => service_id,
                  CreateSerializedURLJob.queue_lookup_key => find_node(service_id))
    job = get_job(job_id)
    blk.call(success(job))
  rescue => e
    wrap_error(e, &blk)
  end

  def import_from_url(service_id, url, &blk)
    @logger.debug("import serialized data from url:#{url} for service_id=#{service_id}")
    svc = @prov_svcs[service_id]
    raise ServiceError.new(ServiceError::NOT_FOUND, service_id) unless svc
    job_id = ImportFromURLJob.create(:service_id => service_id, :url => url,
                  ImportFromURLJob.queue_lookup_key => find_node(service_id))
    job = get_job(job_id)
    blk.call(success(job))
  rescue => e
    wrap_error(e, &blk)
  end

  def import_from_data(service_id, req, &blk)
    @logger.debug("import serialized data from request for service_id=#{service_id}")
    svc = @prov_svcs[service_id]
    raise ServiceError.new(ServiceError::NOT_FOUND, service_id) unless svc
    temp_path = File.join(@upload_temp_dir, "#{service_id}.gz")
    # clean up previous upload
    FileUtils.rm_rf(temp_path)

    File.open(temp_path, "wb+") do |f|
      f.write(Base64.decode64(req.data))
    end
    job_id = ImportFromDataJob.create(:service_id => service_id, :temp_file_path => temp_path,
                                      ImportFromDataJob.queue_lookup_key => find_node(service_id))
    job = get_job(job_id)
    blk.call(success(job))
  rescue => e
    wrap_error(e, &blk)
  end

  def wrap_error(e, &blk)
    @logger.warn(e)
    if e.instance_of? ServiceError
      blk.call(failure(e))
    else
      blk.call(internal_fail)
    end
  end

  def find_node(instance_id)
    svc = @prov_svcs[instance_id]
    raise ServiceError.new(ServiceError::NOT_FOUND, "instance_id #{instance_id}") if svc.nil?
    node_id = svc[:credentials]["node_id"]
    node_id
  end

end
