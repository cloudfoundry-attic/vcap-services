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

require 'worker'

class VCAP::Services::Backup::Rotator

  include VCAP::Services::Backup::Worker

  def initialize(manager, options)
    @manager = manager
    @manager.logger.info("#{self.class}: Initializing")
    @options = options
  end

  def scan(service)
    @manager.logger.info("#{self.class}: Scanning #{service}");
    mysql_extra_prunes = []
    mysql_extra_saves = []
    ins_list = @serv_ins[File.basename(service)]
    # we are expecting the directory structure to look like this
    # root/service/ab/cd/ef/abcdef.../timestamp/data
    each_subdirectory(service) do |ab|
      each_subdirectory(ab) do |cd|
        each_subdirectory(cd) do |ef|
          each_subdirectory(ef) do |guid|
            if ins_list && !ins_list.include?(File.basename(guid))
              if 'mysql' == File.basename(service)
                mysql_extra_prunes |= Dir.entries(guid).delete_if{|x| dotty(x)}
              end
              prune(guid)
            else
              if 'mysql' == File.basename(service)
                mysql_extra_saves |= Dir.entries(guid).delete_if{|x| dotty(x)}
              end
              rotate(guid)
            end
          end
        end
      end
    end
    # special case: for mysql we should take care of system data
    # $root/mysql/{information_schema|mysql}/timestamp
    if service == File.join(@manager.root, "mysql")
      mysql_extra_prunes -= mysql_extra_saves
      mysql_extra_prunes.each do |p|
        prune(File.join(service,"information_schema",p))
        prune(File.join(service,"mysql",p))
      end
      rotate(File.join(service, "information_schema"))
      rotate(File.join(service, "mysql"))
    end
  rescue Interrupt
    raise
  rescue Exception => x
    @manager.logger.error("#{self.class}: Exception while running: #{x.message}, #{x.backtrace.join(', ')}")
  end

  def rotate(dir)
    if File.directory? dir then
      backups = {}
      each_subdirectory(dir) do |backup|
        timestamp = validate(backup)
        if timestamp
          backups[timestamp] = backup
        else
          @manager.logger.warn("Ignoring invalid backup #{backup}")
        end
      end
      prune_all(backups)
    else
      @manager.logger.error("#{self.class}: #{dir} does not exist");
    end
  end

  def validate(path)
    # path is something like:   /root/service/ab/cd/ef/abcdef.../timestamp
    if (path =~ /.+\/(..)\/(..)\/(..)\/(.+)\/(\d+)\Z/)
      prefix = "#{$1}#{$2}#{$3}"
      guid = $4
      timestamp = $5
      return timestamp.to_i if guid =~ /\A#{prefix}/
    elsif path =~ /.+\/mysql\/(information_schema|mysql)\/(\d+)\Z/
      return $2.to_i
    end
    nil
  end

  def prune_all(backups)
    ancient = n_midnights_ago(@options[:max_days])
    latest_time = backups.keys.max
    if latest_time && latest_time<ancient
      #if no backup has been taken place in max_days, then retain
      #the latest one and prune all others
      retain(backups[latest_time],latest_time)
      backups.each do |timestamp,path|
        prune(path,timestamp) if timestamp != latest_time
      end
    else
      midnight = n_midnights_ago(0)
      backups.each do |timestamp, path|
        retain(path, timestamp) if timestamp >= midnight
      end
      bucketize(backups).each do |bucket|
        newest = bucket.max
        bucket.each do |timestamp|
          path = backups[timestamp]
          if timestamp == newest && timestamp >= ancient
            retain(path, timestamp)
          else
            prune(path, timestamp)
          end
        end
      end
    end
  end

  def bucketize(backups)
    # put the timestamps into maxdays + 1 buckets:
    # bucket[maxdays] <-- timestamps older than midnight maxdays ago
    #   ...
    # bucket[i]       <-- timestamps older than midnight i+1 days ago and newer than midhnight i days ago
    #   ...
    # bucket[0]       <-- timestamps older than midnight today and newer than midnight yesterday
    # note that timestamps since midnight today are excluded from all buckets
    maxdays = @options[:max_days]
    buckets = []
    (0 .. maxdays).each { |i|
      buckets << backups.keys.select { |timestamp|
        timestamp < n_midnights_ago(i) && (i == maxdays || timestamp >= n_midnights_ago(i+1))
      }
    }
    buckets
  end

  def n_midnights_ago(n)
    t = Time.at(@manager.time)
    t = t - t.utc_offset # why oh why does Time.at assume local timezone?!
    _, _, _, d, m, y = t.to_a
    t = Time.utc(y, m, d)
    t = t - n * ONE_DAY
    t.to_i
  end

  def retain(path, timestamp)
    @manager.logger.debug("Retaining #{path} from #{Time.at(timestamp)}")
    raise Interrupt, "Interrupted" if @manager.shutdown?
  end

  def prune(path, timestamp=nil )
    if timestamp
      @manager.logger.info("Pruning #{path} from #{Time.at(timestamp)}")
    else
      @manager.logger.info("Pruning #{path} ")
    end
    rmdashr(path)
    # also prune any parent directories that have become empty
    path = parent(path)
    while path != @manager.root && empty(path)
      @manager.logger.info("Pruning empty parent #{path}")
      Dir.delete(path)
      path = parent(path)
    end
  rescue => x
    @manager.logger.error("Could not prune #{path}: #{x.to_s}")
  ensure
    raise Interrupt, "Interrupted" if @manager.shutdown?
  end

end
