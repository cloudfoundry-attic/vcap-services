require 'json'
require 'json_message'
require 'vcap_services_base'

module VCAP
  module Services
    module Backup
    end
  end
end


class VCAP::Services::Backup::JobCleaner
  include VCAP::Services::Base::AsyncJob
  include VCAP::Services::Backup::Util
  def initialize(manager, options, services_redis)
    @manager = manager
    @manager.logger.info("#{self.class}: Initializing")
    @options = options
    VCAP::Services::Base::AsyncJob::Config.redis_config = services_redis
  end

  def run
    @manager.logger.info("#{self.class}: Running. Scanning services redis")
    scan
  end

  def scan
    expiration = n_midnights_ago(@options[:max_days])
    get_all_jobs.each do |jid|
      job = get_job(jid)
      expired = Time.parse(job[:start_time]).to_i < expiration
      if expired
        remove_job(jid)
        if job["status"] == "failed"
          @manager.logger.info("Job failed and expired, cleaning up job [#{jid}]: #{job.inspect}")
        else
          @manager.logger.info("Job expired, cleaning up job [#{jid}]: #{job.inspect}")
        end
      end
    end
  rescue => e
    @manager.logger.error("#{self.class}: Exception while running: #{e.message}, #{e.backtrace.join(', ')}")
  end
end

