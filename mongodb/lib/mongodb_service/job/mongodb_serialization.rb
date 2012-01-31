# Copyright (c) 2009-2011 WMware, Inc.
require "mongodb_service/job/util"
require "mongodb_service/mongodb_error"

module VCAP::Services::Serialization::MongoDB
  include VCAP::Services::Serialization

  # Dump a database into files just as create snapshot job.
  # Create a download token in redis so user is able to download the serialized data.
  class CreateSerializedURLJob < SerializationJob
    VALID_CREDENTIAL_CHARACTERS = ("A".."Z").to_a + ("a".."z").to_a + ("0".."9").to_a
    include VCAP::Services::MongoDB::Util

    def generate_credential(length=12)
      Array.new(length) { VALID_CREDENTIAL_CHARACTERS[rand(VALID_CREDENTIAL_CHARACTERS.length)] }.join
    end

    def perform
      name = options['service_id']
      @logger.info("Begin create serialized url job for #{name}")
      VCAP::Services::Serialization.redis_connect(@config["resque"])

      dump_path = get_serialized_data_path(name)
      cleanup(name)
      FileUtils.mkdir_p(dump_path)
      dump_file_name = File.join(dump_path, "#{name}.tgz")

      result = dump_database(name, dump_file_name)
      raise "Failed to execute dump command to #{name}" unless result

      token = generate_credential()
      service_name = @config['service_name']
      update_download_token(service_name, name, "#{name}.tgz", token)
      url = generate_download_url(name, token)
      @logger.info("Download link generated for #{name}: #{url}")

      job_result = {:url => url}
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

    def generate_download_url(name, token)
      service = @config['service_name']
      url_template = @config["download_url_template"]
      eval "\"#{url_template}\""
    end
  end

  class ImportFromURLJob < SerializationJob
    include VCAP::Services::MongoDB::Util

    def validate_input(file)
      File.size(file) > 0 ? true : nil
    end

    def perform
      name = options['service_id']
      url = options['url']
      @logger.info("Begin import from url:#{url} job for #{name}")

      DataMapper.setup(:default, @config['local_db'])
      DataMapper::auto_upgrade!

      temp_file_path = File.join(@config['tmp_dir'], "#{name}.tgz")
      FileUtils.rm_rf(temp_file_path)
      fetch_url(url, temp_file_path)
      result = validate_input(temp_file_path)
      raise ServiceError.new(MongoDBError::MONGODB_BAD_SERIALIZED_DATA, url) unless result

      result = restore_database(name, temp_file_path)
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
      FileUtils.rm_rf(temp_file_path) if temp_file_path
    end
  end

  # Import serailzed data, which is saved in temp file, into database
  class ImportFromDataJob < SerializationJob
    include VCAP::Services::MongoDB::Util

    def validate_input(file)
      File.size(file) > 0 ? true : nil
    end

    def perform
      name = options['service_id']
      temp_file_path = options['temp_file_path']
      @logger.info("Begin import from url:#{temp_file_path} job for #{name}")
      raise "Can't find temp file: #{temp_file_path}" unless File.exists? temp_file_path

      DataMapper.setup(:default, @config['local_db'])
      DataMapper::auto_upgrade!

      result = validate_input(temp_file_path)
      raise ServiceError.new(MongoDBError::MONGODB_BAD_SERIALIZED_DATA, url) unless result

      result = restore_database(name, temp_file_path)
      raise "Failed to execute import command to #{name}" unless result

      job_result = { :result => :ok }
      set_status({ :complete_time => Time.now.to_s })
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

