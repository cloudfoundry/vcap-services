# Copyright (c) 2009-2011 VMware, Inc.
require "resque/job_with_status"

module Resque
  extend self
  # Patch Resque so we can determine queue by input args.
  # Job class can define select_queue method and the result will be the queue name.
  def enqueue(klass, *args)
    queue = (klass.respond_to?(:select_queue) && klass.select_queue(*args)) || queue_from_class(klass)
    enqueue_to(queue, klass, *args)
  end

  # Backport from resque master branch, we can remove this method when gem is updated.
  def enqueue_to(queue, klass, *args)
    # Perform before_enqueue hooks. Don't perform enqueue if any hook returns false
    before_hooks = Plugin.before_enqueue_hooks(klass).collect do |hook|
      klass.send(hook, *args)
    end
    return nil if before_hooks.any? { |result| result == false }

    Job.create(queue, klass, *args)

    Plugin.after_enqueue_hooks(klass).each do |hook|
      klass.send(hook, *args)
    end

    return true
  end

  class Status
    # new attributes
    hash_accessor :complete_time
  end
end

# A thin layer wraps resque-status
module VCAP::Services::AsyncJob
  include VCAP::Services::Base::Error

  def self.logger=(logger)
    @logger = logger
  end

  def job_repo_setup(options={})
    raise "AsyncJob requires redis configuration." unless options[:redis]
    @logger.debug("Initialize Resque using #{options}")
    Resque.redis = options[:redis]
    Resque::Status.expire_in = options[:expire] if options[:expire]
  end

  def get_job(jobid)
    res = Resque::Status.get(jobid)
    job_to_json(res)
  end

  def get_job_ids()
    Resque::Status.status_ids
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
