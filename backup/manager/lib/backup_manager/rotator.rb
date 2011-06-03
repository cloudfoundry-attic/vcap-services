# Copyright (c) 2009-2011 VMware, Inc.
require 'time'

class VCAP::Services::Backup::Rotator

  def initialize(manager, options)
    @manager = manager
    @manager.logger.info("#{self.class}: Initializing")
    @options = options
  end

  def run
    @manager.logger.info("#{self.class}: Running");
    each_subdirectory(@manager.root) { |service|
      scan(service)
    }
    true
  rescue Exception => x
    @manager.logger.error("#{self.class}: Exception while running: #{x.message}, #{x.backtrace.join(', ')}")
    false
  end

  def scan(service)
    @manager.logger.info("#{self.class}: Scanning #{service}");
    # we are expecting the directory structure to look like this
    # root/service/ab/cd/ef/abcdef.../timestamp/data
    each_subdirectory(service) { |ab|
      each_subdirectory(ab) { |cd|
        each_subdirectory(cd) { |ef|
          each_subdirectory(ef) { |guid|
            rotate(guid)
          }
        }
      }
    }
    # special case: for mysql we should take care of system data
    # $root/mysql/{information_schema|mysql}/timestamp
    if service == File.join(@manager.root, "mysql")
      rotate(File.join(service, "information_schema"))
      rotate(File.join(service, "mysql"))
    end
  rescue Exception => x
    @manager.logger.error("#{self.class}: Exception while running: #{x.message}, #{x.backtrace.join(', ')}")
  end

  def rotate(dir)
    if File.directory? dir then
      backups = {}
      each_subdirectory(dir) { |backup|
        timestamp = validate(backup)
        if timestamp
          backups[timestamp] = backup
        else
          @manager.logger.warn("Ignoring invalid backup #{backup}")
        end
      }
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
    midnight = n_midnights_ago(0)
    backups.each { |timestamp,path|
      retain(path, timestamp) if timestamp >= midnight
    }
    ancient = n_midnights_ago(@options[:max_days])
    bucketize(backups).each { |bucket|
      newest = bucket.max
      bucket.each { |timestamp|
        path = backups[timestamp]
        if timestamp==newest && timestamp>=ancient
          retain(path, timestamp)
        else
          prune(path, timestamp)
        end
      }
    }
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
        timestamp < n_midnights_ago(i) && (i==maxdays || timestamp >= n_midnights_ago(i+1))
      }
    }
    buckets
  end

  ONE_DAY = 24*60*60

  def n_midnights_ago(n)
    t = Time.at(@manager.time)
    t = t - t.utc_offset # why oh why does Time.at assume local timezone?!
    _, _, _, d, m, y = t.to_a
    t = Time.utc(y, m, d)
    t = t - n*ONE_DAY
    t.to_i
  end

  def retain(path, timestamp)
    @manager.logger.debug("Retaining #{path} from #{Time.at(timestamp)}")
  end

  def prune(path, timestamp)
    @manager.logger.info("Pruning #{path} from #{Time.at(timestamp)}")
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
  end

  def rmdashr(path)
    if File.directory?(path)
      Dir.entries(path).each { |child|
        rmdashr(File.join(path, child)) unless dotty(child)
      }
      Dir.delete(path)
    else
      File.delete(path)
    end
  end

  def each_subdirectory(directory, &blk)
    if blk
      Dir.foreach(directory) { |child|
        unless dotty(child)
          path = File.join(directory, child)
          blk.call(path) if File.directory?(path)
        end
      }
    end
  end

  def dotty(s)
    s=='.' || s=='..'
  end

  def empty(path)
    Dir.entries(path).length < 3 # . & ..
  end

  def parent(path)
    File.absolute_path('..', path)
  end

end
