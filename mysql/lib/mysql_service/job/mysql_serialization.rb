# Copyright (c) 2009-2011 VMware, Inc.
$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..')
require "util"
require "mysql_error"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..')
require "mysql_service/node"

module VCAP::Services::Serialization::Mysql
  include VCAP::Services::Serialization

  # Validate the serialized data file.
  # TODO add more validation
  def validate_input(file_path)
    File.open(file_path) do |f|
      return nil unless f.size > 0
    end
    true
  end

  def init_localdb(database_url)
    DataMapper.setup(:default, database_url)
  end

  def mysql_provisioned_service
    VCAP::Services::Mysql::Node::ProvisionedService
  end


  # Dump a database into files just as create snapshot job.
  # Create a download token in redis so user is able to download the serialzed data.
  class CreateSerializedURLJob < SerializationJob
    include VCAP::Services::Mysql::Util

    def perform
      name = options["service_id"]
      @logger.info("Begin create serialized url job for #{name}")
      VCAP::Services::Serialization.redis_connect(@config["resque"])

      dump_path = get_serialized_data_path(name)
      # Clean up previous data
      cleanup(name)
      FileUtils.mkdir_p(dump_path)
      dump_file_name = "#{name}.gz"
      dump_file_path = File.join(dump_path, dump_file_name)

      mysql_conf = @config["mysql"]
      result = dump_database(name, mysql_conf, dump_file_path, :mysqldump_bin => @config["mysqldump_bin"], :gzip_bin => @config["gzip_bin"])
      raise "Failed to execute dump command to #{name}" unless result

      token = generate_credential()
      service_name = @config["service_name"]
      update_download_token(service_name, name, dump_file_name, token)
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

    def generate_download_url(name, token)
      service = @config["service_name"]
      url_template = @config["download_url_template"]
      eval "\"#{url_template}\""
    end
  end

  # Download serialized data from url and import into database
  class ImportFromURLJob < SerializationJob
    include VCAP::Services::Mysql::Util
    include VCAP::Services::Serialization::Mysql

    def perform
      name = options["service_id"]
      url = options["url"]
      @logger.info("Begin import from url:#{url} job for #{name}")
      init_localdb(@config["local_db"])

      @temp_file_path = File.join(@config["tmp_dir"], "#{name}.gz")
      FileUtils.rm_rf(@temp_file_path)
      fetch_url(url, @temp_file_path)
      result = validate_input(@temp_file_path)
      raise ServiceError.new(MysqlError::MYSQL_BAD_SERIALIZED_DATA, url) unless result

      mysql_conf = @config["mysql"]
      srv = mysql_provisioned_service.get(name)
      # to isolate the affection of user uploaded sql file, use instance account to import dump file.
      instance_user = srv.user
      instance_pass = srv.password

      result = import_dumpfile(name, mysql_conf, instance_user, instance_pass, @temp_file_path, :mysql_bin => @config["mysql_bin"], :gzip_bin => @config["gzip_bin"])
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
    include VCAP::Services::Mysql::Util
    include VCAP::Services::Serialization::Mysql

    def perform
      name = options["service_id"]
      @temp_file_path = options["temp_file_path"]
      @logger.info("Begin import from temp_file_path:#{@temp_file_path} job for #{name}")
      init_localdb(@config["local_db"])

      raise "Can't find temp file: #{@temp_file_path}" unless File.exists? @temp_file_path
      result = validate_input(@temp_file_path)
      raise ServiceError.new(MysqlError::MYSQL_BAD_SERIALIZED_DATA, url) unless result

      mysql_conf = @config["mysql"]
      srv = mysql_provisioned_service.get(name)
      instance_user = srv.user
      instance_pass = srv.password

      result = import_dumpfile(name, mysql_conf, instance_user, instance_pass, @temp_file_path, :mysql_bin => @config["mysql_bin"], :gzip_bin => @config["gzip_bin"])
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
