# Copyright (c) 2009-2011 VMware, Inc.
require "resque/job_with_status"
require "fileutils"
require "curb"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..', '..', 'base', 'lib')
require "base/service_error"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..')
require "util"
require "mysql_error"

module VCAP
  module Services
    module Serialization
    end
  end
end

module VCAP::Services::Serialization::Mysql
  extend self

  KEY_PREFIX = "vcap:serialization:mysql".freeze

  def redis=(redis)
    raise "Serialization requires redis configuration." unless redis
    @redis = redis
  end

  def redis_key(key)
    "#{KEY_PREFIX}:#{key}"
  end

  def logger=(logger)
    @logger = logger
    CreateSerializedURLJob.logger = @logger
    ImportFromURLJob.logger = @logger
    ImportFromDataJob.logger = @logger
  end

  class SerializationJob < Resque::JobWithStatus
    include VCAP::Services::Serialization::Mysql
    include VCAP::Services::Base::Error
    include VCAP::Services::Mysql::Util
    include VCAP::Services::Mysql

    def self.logger=(logger)
      @logger = logger
    end

    def self.queue_lookup_key
      :node_id
    end

    def self.select_queue(*args)
      result = nil
      args.each do |arg|
        result = arg[queue_lookup_key]if (arg.is_a? Hash )&& (arg.has_key?(queue_lookup_key))
      end
      @logger.info("Select queue #{result} for #{self} with args:#{args.inspect}")
      result
    end

    def make_logger
      return @logger if @logger
      @logger = Logger.new( STDOUT)
      @logger.level = Logger::DEBUG
      @logger
    end

    # the serialize path structure looks like <base-dir>\serialize\<service-name>\<aa>\<bb>\<cc>\
    # <aabbcc-rest-of-instance-guid>\<serialization data>
    def get_serialized_data_path(name)
      File.join(@config["serialization_base_dir"], "serialize", @config["service_name"] , name[0,2],name[2,2], name[4,2], name)
    end

    protected
    def parse_config
      @config = Yajl::Parser.parse(ENV['WORKER_CONFIG'])
      raise "Need environment variable: WORKER_CONFIG" unless @config
    end

    def redis_connect
      resque = %w(host port password).inject({}){|res, o| res[o.to_sym] = @config["resque"][o]; res}
      @redis = Redis.new(resque)
      redis_init
    end

    #initialze necessary keys
    def redis_init
    end

    def cleanup(name)
      return unless name
      FileUtils.rm_rf(File.join(get_serialized_data_path(name), '.'))
    end

    # Validate downloaded file
    # TODO more restrict validation
    def validate_input(file_path)
      File.open(file_path) do |f|
        return nil unless f.size > 0
      end
      true
    end

    # Fetch remote uri and stream content to file.
    def fetch_url(url, file_path)
      # TODO check the file size before download?
      File.open(file_path, "wb+") do |f|
        c = Curl::Easy.new(url)
        c.on_body{|data| f.write(data)}
        c.perform
      end
    end
  end

  # Dump a database into files just as create snapshot job.
  # Create a download token in redis so user is able to download the serialzed data.
  class CreateSerializedURLJob < SerializationJob

    def perform
      name = options["service_id"]
      make_logger
      @logger.info("Begin create serialized url job for #{name}")
      parse_config
      redis_connect

      dump_path = get_serialized_data_path(name)
      FileUtils.mkdir_p(dump_path)
      # Clean up previous data
      cleanup(name)
      dump_file_name = File.join(dump_path, "#{name}.gz")

      mysql_conf = @config["mysql"]
      result = dump_database(name, mysql_conf, dump_file_name, :mysqldump_bin => @config["mysqldump_bin"], :gzip_bin => @config["gzip_bin"])
      raise "Failed to execute dump command to #{name}" unless result

      token = generate_credential()
      update_token(name, token)
      url = generate_download_url(name, token)
      @logger.info("Download link generated for #{name}: #{url}")

      job_result = { :url => url }
      set_status({:complete_time => Time.now.to_s})
      completed(Yajl::Encoder.encode(job_result))
    rescue => e
      @logger.error("Error in CreateSerializedURLJob #{@uuid}:#{fmt_error(e)}")
      cleanup(name)
      err = (e.instance_of?(ServiceError)? e : ServiceError.new(ServiceError::INTERNAL_ERROR)).to_hash
      err_msg = Yajl::Encoder.encode(err)
      set_status({:complete_time => Time.now.to_s})
      failed(err_msg)
    end

    def update_token(name, token)
      @redis.set(redis_key("token:#{name}"), token)
    end

    def generate_download_url(name, token)
      service = @config["service_name"]
      url_template = @config["download_url_template"]
      eval "\"#{url_template}\""
    end
  end


  # Download serialized data from url and import into database
  class ImportFromURLJob < SerializationJob

    def perform
      name = options["service_id"]
      url = options["url"]
      make_logger
      @logger.info("Begin import from url:#{url} job for #{name}")
      parse_config

      @temp_file_path = File.join(@config["tmp_dir"], "#{name}.gz")
      FileUtils.rm_rf(@temp_file_path)
      fetch_url(url, @temp_file_path)
      result = validate_input(@temp_file_path)
      raise ServiceError.new(MysqlError::MYSQL_BAD_SERIALIZED_DATA, url) unless result

      mysql_conf = @config["mysql"]
      result = import_dumpfile(name, mysql_conf, @temp_file_path, :mysql_bin => @config["mysql_bin"], :gzip_bin => @config["gzip_bin"])
      raise "Failed to execute import command to #{name}" unless result

      job_result = { :result => :ok }
      set_status({:complete_time => Time.now.to_s})
      completed(Yajl::Encoder.encode(job_result))
    rescue => e
      @logger.error("Error in ImportFromURLJob #{@uuid}:#{fmt_error(e)}")
      err = (e.instance_of?(ServiceError)? e : ServiceError.new(ServiceError::INTERNAL_ERROR)).to_hash
      err_msg = Yajl::Encoder.encode(err)
      set_status({:complete_time => Time.now.to_s})
      failed(err_msg)
    ensure
      FileUtils.rm_rf(@temp_file_path) if @temp_file_path
    end
  end

  # Import serailzed data, which is saved in temp file, into database
  class ImportFromDataJob < SerializationJob

    def perform
      name = options["service_id"]
      @temp_file_path = options["temp_file_path"]
      make_logger
      @logger.info("Begin import from temp_file_path:#{@temp_file_path} job for #{name}")
      parse_config

      raise "Can't find temp file: #{@temp_file_path}" unless File.exists? @temp_file_path
      result = validate_input(@temp_file_path)
      raise ServiceError.new(MysqlError::MYSQL_BAD_SERIALIZED_DATA, url) unless result

      mysql_conf = @config["mysql"]
      result = import_dumpfile(name, mysql_conf, @temp_file_path, :mysql_bin => @config["mysql_bin"], :gzip_bin => @config["gzip_bin"])
      raise "Failed to execute import command to #{name}" unless result

      job_result = { :result => :ok }
      set_status({:complete_time => Time.now.to_s})
      completed(Yajl::Encoder.encode(job_result))
    rescue => e
      @logger.error("Error in ImportFromDataJob #{@uuid}:#{fmt_error(e)}")
      err = (e.instance_of?(ServiceError)? e : ServiceError.new(ServiceError::INTERNAL_ERROR)).to_hash
      err_msg = Yajl::Encoder.encode(err)
      set_status({:complete_time => Time.now.to_s})
      failed(err_msg)
    ensure
      FileUtils.rm_rf(@temp_file_path) if @temp_file_path
    end
  end
end
