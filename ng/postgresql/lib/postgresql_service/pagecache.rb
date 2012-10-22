# Copyright (c) 2009-2012 VMware, Inc
require 'rubygems'
require 'bundler/setup'
require 'fadvise'

module VCAP; module Services; module Postgresql; end; end; end

module VCAP::Services::Postgresql::Pagecache

  def setup_image_cache_cleaner(options)
    return unless options[:use_warden] && options[:filesystem_quota] && options[:image_dir]
    @clean_image_cache = options[:clean_image_cache] || false
    if @clean_image_cache
      # Default limit is 40GB
      @clean_image_cache_size_limit = options[:clean_image_cache_size_limit] || 40960000000000
      @clean_image_cache_follow_symlink = options[:clean_image_cache_follow_symlink] || false
      @clean_image_cache_interval = options[:clean_image_cache_interval] || 3
      EM.add_periodic_timer(@clean_image_cache_interval){ evict_file_cache(options[:image_dir]) }
    end
  end

  def evict_file_cache(file)
    begin
      if File.symlink?(file)
        if @clean_image_cache_follow_symlink
          @logger.debug("#{file} is a symbolic link, we will follow it.")
          file = File.realpath(file)
        else
          @logger.warn("#{file} is a symbolic link, can't follow to clean its page cache.")
          return false
        end
      end

      unless File.exist? file
        @logger.warn("#{file} does not exist, could not clean its page cache.")
        return false
      end
      if File.directory?(file)
        Dir.foreach(file) do |item|
          next if item == '.' or item == '..'
          evict_file_cache(File.join(file, item))
        end
      else
        file_len = File.size(file)

        if file_len > @clean_image_cache_size_limit
          @logger.warn("#{file} exceeds the limit, could not clean its page cache.")
          return false
        end

        @logger.debug("Evicting #{file}...")
        File.open(file, 'r') do |f|
          f.fadvise(0, file_len, :dont_need)
        end
      end
    rescue => e
      @logger.error("Fail to evict page cache of #{file} for #{e}:#{e.backtrace.join('|')}")
    end
  end
end
