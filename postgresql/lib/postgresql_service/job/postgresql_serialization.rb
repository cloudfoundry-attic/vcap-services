# Copyright (c) 2009-2011 VMware, Inc.

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..')
require "util"
require "postgresql_error"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..')
require "postgresql_service/node"

module VCAP::Services::Postgresql::Serialization
  include VCAP::Services::Base::AsyncJob::Serialization

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
  class CreateSerializedURLJob < BaseCreateSerializedURLJob

    include VCAP::Services::Postgresql::Util
    include VCAP::Services::Postgresql::Serialization

    def execute
      dump_file_name = serialize_db(name)

      {:dump_file_name => dump_file_name}
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
      dump_file_name
    end
  end

  # Download serialized data from url and import into database
  class ImportFromURLJob < BaseImportFromURLJob

    include VCAP::Services::Postgresql::Util
    include VCAP::Services::Postgresql::Serialization

    def execute
      import_db_from_url(name, url)

      true
    end

    def import_db_from_url(name, url)
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
  class ImportFromDataJob < BaseImportFromDataJob

    include VCAP::Services::Postgresql::Util
    include VCAP::Services::Postgresql::Serialization

    def execute
      import_db_from_file(name)

      true
    end

    def import_db_from_file(name)
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
