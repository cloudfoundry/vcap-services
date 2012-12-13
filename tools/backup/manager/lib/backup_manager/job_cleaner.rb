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
  def initialize(manager, options)
    @manager = manager
    @manager.logger.info("#{self.class}: Initializing")
    @options = options
  end

  def run
    @manager.logger.info("#{self.class}: Running")
    scan
  end

  def scan
    @manager.logger.info("#{self.class}: Scanning queue")
    timeout_jobs=[]
    failed_jobs=[]
    get_all_jobs.each do |job|
      if Time.parse(get_job(job)[:start_time]).to_i < n_midnights_ago(@options[:max_days])
        failed_jobs << job if get_job(job)[:status] == "failed"
        timeout_jobs << job 
      end
    end
    log_jobs(failed_jobs, "failed")
    log_jobs(timeout_jobs, "timeout")
    cleanup_jobs(timeout_jobs)
  rescue Exception => x
    @manager.logger.error("#{self.class}: Exception while running: #{x.message}, #{x.backtrace.join(', ')}")
  end
  
  def log_jobs(jobs, status)
    if status == "failed"
      jobs.each do |job_id|
        @manager.logger.info("Job details: #{get_job(job_id).inspect}")
      end
    else
      jobs.each do |job_id|
        @manager.logger.debug("Job [#{job_id}] #{status}")
      end
    end
  end

  def cleanup_jobs(jobs)
    jobs.each do |job_id|
      remove_job(job_id)
      @manager.logger.debug("Delete #{job_id} from queue due to timeout")
    end
  end

  def n_midnights_ago(n)
    t = Time.at(@manager.time)
    t = t - t.utc_offset # why oh why does Time.at assume local timezone?!
    _, _, _, d, m, y = t.to_a
    t = Time.utc(y, m, d)
    t = t - n * ONE_DAY
    t.to_i
  end
end

