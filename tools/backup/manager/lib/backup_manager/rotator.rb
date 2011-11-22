# Copyright (c) 2009-2011 VMware, Inc.
require 'time'
require 'eventmachine'
require 'em-http'
require 'json'
require 'json_message'
require 'services/api'

class VCAP::Services::Backup::Rotator
  @@req = { :head =>
    {
      'Content-Type'         => 'application/json',
      'X-VCAP-Service-Token' => '0xdeadbeef',
    }
  }


  def initialize(manager, options)
    @manager = manager
    @manager.logger.info("#{self.class}: Initializing")
    @options = options
  end

  def run
    if Dir.exists?(@manager.root)
      @manager.logger.info("#{self.class}: Running");
      get_live_service_ins
      each_subdirectory(@manager.root) do |service|
        scan(service)
      end
    else
      @manager.logger.warn("Root directory for scanning is not existed.")
    end
    true
  rescue Interrupt
    raise
  rescue Exception => x
    @manager.logger.error("#{self.class}: Exception while running: #{x.message}, #{x.backtrace.join(', ')}")
    false
  end
  def handle_response(http,name)
    instances = []
    if http.response_header.status == 200
      begin
        resp = VCAP::Services::Api::ListHandlesResponse.decode(http.response)
        resp.handles.each do |h|
          if h['service_id']
            service_id = h['service_id']
            #Because some old service_ids may initial with service name,
            #remove the service name for compatibility's sake.
            service_id.gsub!(/^(mongodb|redis)-/,'')
            service_id = String.new(service_id)
            instances << service_id if service_id
          end
        end if resp.handles
        @serv_ins[name] = instances
        @manager.logger.debug("Live #{name} instances: #{instances.size}")
      rescue => e
        @manager.logger.error("Error to parse handle: #{e.message}")
      end
    else
      @manager.logger.warn("Fetching #{name} handle ans: #{http.response_header.status}")
    end
  end

  def request_service_ins_fibered(uri)
    f = Fiber.current
    http = EM::HttpRequest.new(uri).get(@@req)
    http.errback  { f.resume([false,http]) }
    http.callback { f.resume([true,http]) }
    Fiber.yield
  end

  def request_service_ins(uri,name)
    http = EM::HttpRequest.new(uri).get(@@req)
    http.callback do
      handle_response(http,name)
      EM.stop
    end
    http.errback do
      @manager.logger.error("Error at fetching handle at #{uri} ans: #{http.error}")
      EM.stop
    end
  end

  def get_live_service_ins
    cc_uri = @options[:cloud_controller_uri]||"api.vcap.me"
    cc_uri = "http://#{cc_uri}" if !cc_uri.start_with?("http://")
    @serv_ins = {}
    @options[:services].each do |name,svc|
      version = svc['version']
      uri = "#{cc_uri}/services/v1/offerings/#{name}-#{version}/handles"
      @@req[:head]['X-VCAP-Service-Token'] = svc['token']||'0xdeadbeef'
      if EM.reactor_running?
        res = request_service_ins_fibered(uri)
        if res[0]
          handle_response(res[1],name)
        else
          @manager.logger.error("Error at fetching handle at #{uri} ans: #{res[1].error}")
        end
      else
        EM.run{
          request_service_ins(uri,name)
        }
      end
      if @manager.shutdown?
        raise Interrupt, "Interrupted"
      end
    end if @options[:services]
  rescue => e
    @manager.logger.error "Failed to get_live_service_ins #{e.message}"
  end



  def scan(service)
    @manager.logger.info("#{self.class}: Scanning #{service}");
    mysql_extra_prunes = []
    mysql_extra_saves = []
    ins_list = @serv_ins[File.basename(service)]
    # we are expecting the directory structure to look like this
    # root/service/ab/cd/ef/abcdef.../timestamp/data
    each_subdirectory(service) { |ab|
      each_subdirectory(ab) { |cd|
        each_subdirectory(cd) { |ef|
          each_subdirectory(ef) { |guid|
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
          }
        }
      }
    }
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
      backups.each { |timestamp,path|
        retain(path, timestamp) if timestamp >= midnight
      }
      bucketize(backups).each { |bucket|
        newest = bucket.max
        bucket.each { |timestamp|
          path = backups[timestamp]
          if timestamp == newest && timestamp >= ancient
            retain(path, timestamp)
          else
            prune(path, timestamp)
          end
        }
      }
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
    if @manager.shutdown?
      raise Interrupt, "Interrupted"
    end
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
    if @manager.shutdown?
      raise Interrupt, "Interrupted"
    end
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
    s == '.' || s == '..'
  end

  def empty(path)
    Dir.entries(path).length < 3 # . & ..
  end

  def parent(path)
    File.absolute_path('..', path)
  end

end
