# Copyright (c) 2009-2011 VMware, Inc.

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..')
require "util"
require "postgresql_error"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..')
require "postgresql_service/node"

module VCAP::Services::Serialization::Postgresql
  include VCAP::Services::Serialization

  # Validate the serialized data file.
  # TODO add more validation
  def validate_input(file_path)
    File.open(file_path) do |f|
      return nil unless f.size > 0
    end
    true
  end

  # Dump a database into files just as create snapshot job.
  # Create a download token in redis so user is able to download the serialzed data.
  class CreateSerializedURLJob < SerializationJob

    include VCAP::Services::Postgresql::Util
    include VCAP::Services::Serialization::Postgresql

    def perform
      name = options["service_id"]
      @logger.info("Begin create serialized url job for #{name}")
      VCAP::Services::Serialization.redis_connect(@config["resque"])

      url = serialize_db(name)

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

    def serialize_db(name)
      dump_path = get_serialized_data_path(name)
      # Clean up previous data
      cleanup(name)
      FileUtils.mkdir_p(dump_path)
      dump_file_name = "#{name}.dump"
      dump_file_path = File.join(dump_path, dump_file_name)

      postgresql_conf = @config["postgresql"]

      # setup DataMapper
      VCAP::Services::Postgresql::Node.setup_datamapper(:default, @config['local_db'])
      # prepare the command
      provisionedservice = VCAP::Services::Postgresql::Node::Provisionedservice.get(name)
      raise "Could not get the service with name #{name}" unless provisionedservice

      default_user = provisionedservice.bindusers.all(:default_user => true)[0]
      if default_user.nil?
        @logger.error("The provisioned service with name #{name} has no default user")
        raise "Failed to serialize database of #{name}"
      end

      user = default_user[:user]
      passwd = default_user[:password]
      host, port = %w(host port).map{ |k| postgresql_conf[k] }
      result = dump_database(name, host, port, user, passwd, dump_file_path, {:dump_bin => @config["dump_bin"], :logger => @logger } )
      raise "Fail to serialize the database #{name} " unless result
      token = generate_credential()
      service_name = @config["service_name"]
      update_download_token(service_name, name, dump_file_name, token)
      url = generate_download_url(name, token)
      @logger.info("Download link generated for #{name}: #{url}")
      url
    end

    def generate_download_url(name, token)
      service = @config["service_name"]
      url_template = @config["download_url_template"]
      eval "\"#{url_template}\""
    end
  end

  # Download serialized data from url and import into database
  class ImportFromURLJob < SerializationJob

    include VCAP::Services::Postgresql::Util
    include VCAP::Services::Serialization::Postgresql

    def perform
      name = options["service_id"]
      url = options["url"]
      @logger.info("Begin import from url:#{url} job for #{name}")
      result = import_db_from_url(name, url)
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

    def import_db_from_url(name, url)
      @temp_file_path = File.join(@config["tmp_dir"], "#{name}.dump")
      FileUtils.rm_rf(@temp_file_path)
      fetch_url(url, @temp_file_path)
      result = validate_input(@temp_file_path)
      raise ServiceError.new(PostgresqlError::POSTGRESQL_BAD_SERIALIZED_DATA, url) unless result

      postgresql_conf = @config["postgresql"]

      VCAP::Services::Postgresql::Node.setup_datamapper(:default ,@config["local_db"])
      service = VCAP::Services::Postgresql::Node::Provisionedservice.get(name)
      raise "Could not get the service with the name #{name}" unless service
      # to isolate the affection of user uploaded sql file, use parent role to import dump file.

      host, port, vcap_user, vcap_pass = %w(host port user pass).map{ |k| postgresql_conf[k] }

      reset_db(host, port, vcap_user, vcap_pass, name, service)

      default_user = service.bindusers.all(:default_user => true)[0]
      user = default_user[:user]
      passwd = default_user[:password]

      result = restore_database(name, host, port, user, passwd, @temp_file_path, { :restore_bin => @config["restore_bin"], :logger => @logger } )
      raise "Failed to execute import command to #{name}" unless result
      result
    end

  end

  # Import serailzed data, which is saved in temp file, into database
  class ImportFromDataJob < SerializationJob

    include VCAP::Services::Postgresql::Util
    include VCAP::Services::Serialization::Postgresql

    def perform
      name = options["service_id"]
      @temp_file_path = options["temp_file_path"]
      @logger.info("Begin import from temp_file_path:#{@temp_file_path} job for #{name}")

      result = import_db_from_file(name)

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

    def import_db_from_file(name)
      raise "Can't find temp file: #{@temp_file_path}" unless File.exists? @temp_file_path
      result = validate_input(@temp_file_path)
      raise ServiceError.new(PostgresqlError::POSTGRESQL_BAD_SERIALIZED_DATA, url) unless result

      postgresql_conf = @config["postgresql"]

      VCAP::Services::Postgresql::Node.setup_datamapper(:default ,@config["local_db"])
      service = VCAP::Services::Postgresql::Node::Provisionedservice.get(name)
      raise "Could not get the service with the name #{name}" unless service
      # to isolate the affection of user uploaded sql file, use parent role to import dump file.

      host, port, vcap_user, vcap_pass = %w(host port user pass).map{ |k| postgresql_conf[k] }

      reset_db(host, port, vcap_user, vcap_pass, name, service)

      default_user = service.bindusers.all(:default_user => true)[0]
      user = default_user[:user]
      passwd = default_user[:password]

      result = restore_database(name, host, port, user, passwd, @temp_file_path, { :restore_bin => @config["restore_bin"], :logger => @logger } )
      raise "Failed to execute import command to #{name}" unless result
      result
    end
  end
end
