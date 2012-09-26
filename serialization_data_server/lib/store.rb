require 'redis'
require 'json'
require 'sys/filesystem'
require 'fileutils'

module VCAP
  module Services
    module Serialization
    end
  end
end

class VCAP::Services::Serialization::Store

  REQ_OPTS = %w(serialization_base_dir redis upload_token).map {|o| o.to_sym}
  VALID_CREDENTIAL_CHARACTERS = ("A".."Z").to_a + ("a".."z").to_a + ("0".."9").to_a

  attr_reader :base_dir

  def initialize(opts)
    missing_opts = REQ_OPTS.select {|o| !opts.has_key? o}
    raise ArgumentError, "Missing options: #{missing_opts.join(', ')}" unless missing_opts.empty?
    @opts = opts
    @upload_token = opts[:upload_token]
    @expire_time = opts[:expire_time] || 600
    @purge_num = opts[:purge_num] || 1000
    @logger = opts[:logger] || make_logger
    @base_dir = opts[:serialization_base_dir]
  end

  def connected?
    @redis != nil
  end

  def redis_key(service, service_id)
    "vcap:snapshot:#{service_id}"
  end

  def redis_file_key(service, service_id)
    "vcap:serialized_file:#{service}:#{service_id}"
  end

  def redis_upload_purge_queue
    "vcap:upload_purge_queue"
  end

  def make_file_world_readable(file)
    begin
      new_permission = File.lstat(file).mode | 0444
      File.chmod(new_permission, file)
    rescue => e
      @logger.error("Fail to make the file #{file} world_readable.")
    end
  end

  def connect_redis
    redis_config = %w(host port password).inject({}){|res, o| res[o.to_sym] = @opts[:redis][o]; res}
    @redis = Redis.connect(redis_config)
  end

  def make_logger()
    logger = Logger.new(STDOUT)
    logger.level = Logger::DEBUG
    logger
  end

  def snapshot_file_path(service, id, snapshot_id, file_name)
    File.join(@base_dir, "snapshots", service, id[0,2], id[2,2], id[4,2], id, snapshot_id, file_name)
  end

  def upload_file_path(service, id, token, time=nil)
    File.join(@base_dir, "uploads", service, id[0,2], id[2,2], id[4,2], id, (time||Time.now.to_i).to_s, token)
  end

  def generate_file_token(service, service_id, file_name, length=12)
    prefix=Array.new(length) { VALID_CREDENTIAL_CHARACTERS[rand(VALID_CREDENTIAL_CHARACTERS.length)] }.join
    appendix=Digest::MD5.hexdigest(@upload_token+service+service_id+file_name+(Time.now.to_i.to_s))
    return prefix+appendix
  end

  def set_expire(service, service_id, token, file_path, time=nil)
    if connected?
      @redis.rpush(redis_upload_purge_queue, {"service" => service, "service_id" => service_id, "token" => token, "file" => file_path, "time" => (time || Time.now.to_i)}.to_json)
    else
      @logger.warn("Redis is not connected, could not run_expire")
    end
  end

  def register_file(service, service_id, token, file_path, time=nil)
    if connected?
      @redis.hset(redis_file_key(service, service_id), token, {"file" => file_path, "time" => (time || Time.now.to_i)}.to_json )
      set_expire(service, service_id, token, file_path, time)
    else
      @logger.warn("Redis is not connected, could not register_file")
    end
  end

  def store_file(service, service_id, ori_file_path)
    file_basename = File.basename(ori_file_path)
    file_token = nil
    new_file_path = nil
    gen_time = Time.now.to_i

    # generate file token
    # In most cases, try once then break, but in some extreme cases, try several times
    loop {
        file_token = generate_file_token(service, service_id, file_basename)
        new_file_path = upload_file_path(service, service_id, file_token, gen_time)
        break unless File.exist?(new_file_path)
    }

    unless new_file_path && FileUtils.mkdir_p(File.dirname(new_file_path))
      @logger.error("Failed to create directory to store the uploaded file #{ori_file_path}")
      return [400, nil, nil]
    end

    # move the file to the upload file
    FileUtils.mv(ori_file_path, new_file_path)
    unless File.exist?(new_file_path)
      @logger.error("Failed to move the uploaded file #{ori_file_path} to the new localtion #{new_file_path}")
      return [400, nil, nil]
    end

    # register the file into redis
    unless file_token && register_file(service, service_id, file_token, new_file_path, gen_time)
      @logger.error("Fail to register the uploaded file #{new_file_path} to redis: #{service} #{service_id} using token #{file_token}")
      @logger.info("Cleanup the file #{new_file_path}")
      FileUtils.rm_rf(new_file_path) if new_file_path
      return [400, nil, nil]
    end
    return [200, file_token, new_file_path]
  end

  def get_file(service, service_id, token)
    if connected?
      @redis.hget(redis_file_key(service, service_id), token)
    else
      @logger.warn("Redis is not connected, could not get file")
      nil
    end
  end

  def try_unregister_file(service, service_id, token, greedy=false)
    file = nil
    time = nil

    unless connected?
      @logger.warn("Redis is not connected, could not unregister file")
      return [file, time]
    end

    file_info_s = get_file(service, service_id, token)
    if file_info_s
      begin
        file_info = JSON.parse(file_info_s)
        file = file_info["file"]
        time = file_info["time"]
        if file && time && (greedy == true || (@expire_time > 0 && (Time.now.to_i - time.to_i) > @expire_time))
          @logger.debug("[try_unregister_file] Start to delete file #{file} for service #{service} #{service_id} and unregister it with token #{token}.")
          FileUtils.rm_rf(file)
          @redis.hdel(redis_file_key(service, service_id), token)
          @logger.debug("[try_unregister_file] Done to delete file #{file} for service #{service} #{service_id} and unregister it with token #{token}.")
          file = nil
          time = nil
        end
      rescue => e
        @logger.error("When trying to unregistering file #{file_info_s.inspect}, met error #{e.backtrace.join('|')}")
        file = nil
        # if we met exception when deleting file, keep time not nil to figure out
      end
    end
    [file, time]
  end

  def purge_expired
    unless connected?
      @logger.warn("You should connect to redis first before purging expired")
      return
    end
    expired_line = Time.now.to_i - @expire_time
    index= 0
    until index == @purge_num
      expired_file= @redis.lpop(redis_upload_purge_queue)
      if expired_file
        begin
          file = JSON.parse(expired_file)
          time = file["time"]
          if time && time.to_i < expired_line
            @logger.debug("[purge_expired] Start to delete file #{file["file"]} for service #{file["service"]} #{file["service_id"]} and unregister it with token #{file["token"]}.")
            FileUtils.rm_rf(file["file"])
            @redis.hdel(redis_file_key(file["service"], file["service_id"]), file["token"])
            @logger.debug("[purge_expired] Done to delete file #{file["file"]} for service #{file["service"]} #{file["service_id"]} and unregister it with token #{file["token"]}.")
          elsif time
            begin
              # no staled files
              @redis.lpush(redis_upload_purge_queue, expired_file)  # push back
              break
            rescue => e
              @logger.error("When push back non-expired file #{expired_file}, met error #{e.backtrace.join('|')}")
            end
          else
            @logger.error("When purging expired file #{expired_file}, timestamp is nil.")
          end
        rescue => e
          @logger.error("When purging expired file #{expired_file}, met error #{e.backtrace.join('|')}")
        end
      else
        # empty queue
        break
      end
      index += 1
    end
  end

  def get_snapshot_file_path(service, service_id, snapshot_id, token)
    unless connected?
      @logger.warn("Redis is not connected, could not get snapshot file")
      return [500, nil, nil]
    end

    key = redis_key(service, service_id)
    result = @redis.hget(key, snapshot_id)

    if not result
      @logger.info("Can't find snapshot info for service=#{service}, service_id=#{service_id}, snapshot=#{snapshot_id}")
      return [404, nil, nil]
    end

    result = Yajl::Parser.parse(result)
    return [403, nil, nil] unless token == result["token"]
    file_name = result["file"]
    if not file_name
      @logger.error("Can't get serialized filename from redis using key:#{key}.")
      return [501, nil, nil]
    end
    real_path = snapshot_file_path(service, service_id, snapshot_id, file_name)
    [200, real_path, file_name]
  end
end
