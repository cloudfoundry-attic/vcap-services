# Copyright (c) 2009-2012 VMware, Inc
require 'rubygems'
require 'bundler/setup'
require 'fadvise'

module VCAP; module Services; module Postgresql; end; end; end

module VCAP::Services::Postgresql::Pagecache

  def setup_image_cache_cleaner(options)
    return unless options[:use_warden] && options[:filesystem_quota] && options[:image_dir]
    if options[:clean_image_cache]
      @clean_image_cache_interval = options[:clean_image_cache_interval] || 3
      EM.add_periodic_timer(@clean_image_cache_interval){ evict_file_cache(options[:image_dir]) }
    end
  end

  def evict_file_cache(file)
    begin
      unless File.exist? file
        @logger.warn("#{file} does not exist, could not clean its page cache.")
        return false
      end

      if File.symlink?(file)
        @logger.debug("#{file} is a symbolic link, won't follow to clean its page cache.")
        return false
      end

      if File.directory?(file)
        Dir.foreach(file) do |item|
          next if item == '.' or item == '..'
          evict_file_cache(File.join(file, item))
        end
      else
        file_len = File.size(file)
        File.open(file, 'r') do |f|
          f.fadvise(0, file_len, :dont_need)
        end
      end
    rescue => e
      @logger.error("Fail to evict page cache of #{file} for #{e}:#{e.backtrace.join('|')}")
    end
  end
end
