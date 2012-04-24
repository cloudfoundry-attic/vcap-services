# Copyright (c) 2009-2011 VMware, Inc.
require "resque-status"

$LOAD_PATH.unshift File.dirname(__FILE__)
require "config"
$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..')
require "service_error"

module Resque
  extend self
  # Patch Resque so we can determine queue by input args.
  # Job class can define select_queue method and the result will be the queue name.
  def enqueue(klass, *args)
    queue = (klass.respond_to?(:select_queue) && klass.select_queue(*args)) || queue_from_class(klass)
    enqueue_to(queue, klass, *args)
  end

end

module Resque::Plugins::Status
    class Hash
      # new attributes
      hash_accessor :complete_time
    end
end

# A thin layer wraps resque-status
module VCAP::Services::Base::AsyncJob
  include VCAP::Services::Base::Error

  def job_repo_setup
    redis = Config.redis
    @logger = Config.logger
    raise "AsyncJob requires redis configuration." unless redis
    @logger.debug("Initialize Resque using #{redis}") if @logger
    ::Resque.redis = redis
  end

  def get_job(jobid)
    res = Resque::Plugins::Status::Hash.get(jobid)
    job_to_json(res)
  end

  def get_all_jobs()
    Resque::Plugins::Status::Hash.keys
  end

  def job_to_json(job)
    return nil unless job
    res = {
      :job_id => job.uuid,
      :status => job.status,
      :start_time => job.time.to_s,
      :description => job.options[:description] || "None"
    }
    res[:complete_time] = job.complete_time if job.complete_time
    res[:result] = validate_message(job.message) if job.message
    res
  end

  def validate_message(msg)
    Yajl::Parser.parse(msg)
  rescue => e
    # generate internal error if we can't parse err msg
    ServiceError.new(ServiceError::INTERNAL_ERROR).to_hash
  end
end
