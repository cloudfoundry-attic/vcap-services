# Copyright (c) 2009-2011 VMware, Inc.
require 'time'
require 'em-http'
require 'json'
require 'json_message'
require 'services/api'
require 'fiber'

module VCAP
  module Services
    module Backup
    end
  end
end

require 'util'

class VCAP::Services::Backup::SnapshotCleaner

  include VCAP::Services::Backup::Worker

  def initialize(manager, options)
    @manager = manager
    @manager.logger.info("#{self.class}: Initializing")
    @options = options
    @options[:greedy_mark] = false if @options[:greedy_mark].nil?
  end

  def scan(root)
    # we are expecting the directory structure to look like this
    # snapshot cleaner: root/snapshots/service/ab/cd/ef/abcdef.../snapshot_id/packaged_file
    # upload file cleaner: root/uploads/service/ab/cd/ef/abcdef.../snapshot_id/packaged_file

    each_subdirectory(root) do |job|
      if((job =~ /uploads\Z/).nil? && (job =~ /snapshots\Z/).nil?)
        next
      end
      each_subdirectory(job) do |service|
        # scan if we could get correct instance list for the service
        @manager.logger.info("#{self.class}: Scanning #{service}");
        ins_list = @serv_ins[File.basename(service)]
        each_subdirectory(service) do |ab|
          each_subdirectory(ab) do |cd|
            each_subdirectory(cd) do |ef|
              each_subdirectory(ef) do |guid|
                if (!ins_list || (ins_list && !ins_list.include?(File.basename(guid))))
                  cleanup(guid)
                else
                  noop(guid, ins_list && ins_list.include?(File.basename(guid)))
                end
              end
            end
          end
        end
      end
    end
  rescue Interrupt
    raise
  rescue Exception => x
    @manager.logger.error("#{self.class}: Exception while running: #{x.message}, #{x.backtrace.join(', ')}")
  end

  def noop(dir, need_delete_mark)
    if need_delete_mark
      mark_file=File.join(dir, "last_clean_time")
      rmdashr(mark_file) if File.exists?(mark_file)
    end
  rescue => x
    @manager.logger.error("Fail to delete the mark file in noop in #{dir}: #{x.to_s}")
  ensure
    raise Interrupt, "Interrupted" if @manager.shutdown?
  end

  def cleanup(dir)
    if File.directory? dir then
      dirs = {}
      each_subdirectory(dir) do |subdir|
        id = validate(subdir)
        if !id.nil?
          dirs[id] = subdir
        else
          @manager.logger.warn("Ignoring invalid directory #{subdir}")
        end
      end
      try_cleanup(dir, dirs)
    else
      @manager.logger.error("#{self.class}: #{dir} does not exist");
    end
  rescue => x
    @manager.logger.error("Could not try to cleanup #{dir}: #{x.to_s}")
  ensure
    raise Interrupt, "Interrupted" if @manager.shutdown?
  end

  def validate(path)
    # path is something like: /root/snapshots/service/ab/cd/ef/abcdef.../snapshot_id
    # or: /root/uploads/service/ab/cd/ef/abcdef.../timestamp
    if (path =~ /.+\/(..)\/(..)\/(..)\/(.+)\/(\d+)\Z/)
      prefix = "#{$1}#{$2}#{$3}"
      guid = $4
      key = $5
      return key.to_i if guid =~ /\A#{prefix}/
    else
      nil
    end
  end

  def get_latest_key(dir, jobs)
    latest_key = nil
    sorted_keys =  jobs.keys.sort { |x, y| y <=> x }
    sorted_keys.each do |key|
      job_dir = File.join(dir, key.to_s)
      begin
        if Dir.entries(job_dir).size > 2
          latest_key = key
          break
        end
      rescue => e
        @manager.logger.warn("Fail to list the entries in #{job_dir}")
      end
    end
    latest_key
  end

  # cleanup all snapshots or uploaded files
  def all_cleanup(dir, jobs)
    prune(dir) if File.exists?(dir)
    @manager.logger.info("The directory #{dir} and files/subdirs under it are all deleted.")
  rescue => x
    @manager.logger.error("Could not cleanup all files/subdirs under #{dir}: #{x.backtrace}")
  ensure
    raise Interrupt, "Interrupted" if @manager.shutdown?
  end

  # keep marked
  def keep_mark(dir, jobs)
    @manager.logger.debug("Keep the clean mark under #{dir}")
  ensure
    raise Interrupt, "Interrupted" if @manager.shutdown?
  end

  # cleanup the snapshots or uploaded files except the latest one and mark the timestamp in a file
  def mark_cleanup(dir, jobs, latest_key=nil, previous_marktime=nil)
    @manager.logger.info("First try to cleanup the files/subdirs under #{dir}, mark it first.")
    mark_file = File.join(dir, "last_clean_time")
    File.open(mark_file, "w") do |file|
      mark="#{@manager.time.to_s}|#{latest_key}|#{previous_marktime}"
      file.write(mark)
    end
    if @options[:greedy_mark] && latest_key.nil? == false
      @manager.logger.info("greedy_mark is true, will delete all files/subdirs except the latest one")
      jobs.keys.each do |key|
        unless key == latest_key
          job_dir = File.join(dir, key.to_s)
          rmdashr(job_dir) if File.exists?(job_dir)
          @manager.logger.info("Files/subdirs in #{job_dir} is deleted.")
        end
      end
    end
  rescue => x
    @manager.logger.error("Could not cleanup and mark files under #{dir}: #{x.to_s}")
  ensure
    raise Interrupt, "Interrupted" if @manager.shutdown?
  end

  def try_cleanup(dir, jobs)
    mark_file = File.join(dir, "last_clean_time")
    if File.exists?(mark_file)
      # not the first time to cleanup
      # get the last cleanup time
      last_clean_time = nil
      mark_line = nil
      File.open(mark_file, "r") do |file|
        mark_line = file.read()
      end

      if mark_line.nil? == false
        tmp = mark_line.split('|')
        last_clean_time = tmp[0] if tmp.length >= 1
      end

      if last_clean_time =~ /\A(\d+)\Z/
        # check whether exceed the survival time
        if last_clean_time.to_i < n_midnights_ago(@options[:max_days])
          @manager.logger.warn("The file/subdirs under #{dir} are too old and this is not the first try to cleanup, all of them will be deleted.")
          all_cleanup(dir, jobs)
        else
          keep_mark(dir, jobs)
        end
      else
        @manager.logger.warn("Try to cleanup the file/subdirs under #{dir} but the mark file has invalid content, redo mark_cleanup.")
        latest_key = get_latest_key(dir, jobs)
        mark_cleanup(dir, jobs, latest_key, last_clean_time.nil? ? "0" : last_clean_time)
      end
    else
      # the first time to cleanup
      # keep the latest snapshot or uploaded file  and delete all others
      latest_key = get_latest_key(dir, jobs)
      # keep the latest snapshot or uploaded file and mark the timestamp to a file named last_clean_time
      mark_cleanup(dir, jobs, latest_key)
    end
  rescue Interrupt
    raise
  end
end
