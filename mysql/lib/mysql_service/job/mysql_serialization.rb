# Copyright (c) 2009-2011 VMware, Inc.
$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..')
require "util"
require "mysql_error"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..')
require "mysql_service/node"

module VCAP::Services::Mysql::Serialization
  include VCAP::Services::Base::AsyncJob::Serialization

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
  class CreateSerializedURLJob < BaseCreateSerializedURLJob
    include VCAP::Services::Mysql::Util

    def execute
      dump_path = get_serialized_data_path(name)
      # Clean up previous data
      cleanup(name)
      FileUtils.mkdir_p(dump_path)
      dump_file_name = "#{name}.gz"
      dump_file_path = File.join(dump_path, dump_file_name)

      mysql_conf = @config["mysql"]
      result = dump_database(name, mysql_conf, dump_file_path, :mysqldump_bin => @config["mysqldump_bin"], :gzip_bin => @config["gzip_bin"])
      raise "Failed to execute dump command to #{name}" unless result

      {:dump_file_name => dump_file_name}
    end
  end

  # Download serialized data from url and import into database
  class ImportFromURLJob < BaseImportFromURLJob
    include VCAP::Services::Mysql::Util
    include VCAP::Services::Mysql::Serialization

    def execute
      init_localdb(@config["local_db"])
      mysql_conf = @config["mysql"]
      srv = mysql_provisioned_service.get(name)
      # to isolate the affection of user uploaded sql file, use instance account to import dump file.
      instance_user = srv.user
      instance_pass = srv.password

      result = import_dumpfile(name, mysql_conf, instance_user, instance_pass, @temp_file_path, :mysql_bin => @config["mysql_bin"], :gzip_bin => @config["gzip_bin"])
      raise "Failed to execute import command to #{name}" unless result

      true
    end
  end

  # Import serailzed data, which is saved in temp file, into database
  class ImportFromDataJob < BaseImportFromDataJob
    include VCAP::Services::Mysql::Util
    include VCAP::Services::Mysql::Serialization

    def execute
      init_localdb(@config["local_db"])
      mysql_conf = @config["mysql"]
      srv = mysql_provisioned_service.get(name)
      instance_user = srv.user
      instance_pass = srv.password

      result = import_dumpfile(name, mysql_conf, instance_user, instance_pass, @temp_file_path, :mysql_bin => @config["mysql_bin"], :gzip_bin => @config["gzip_bin"])
      raise "Failed to execute import command to #{name}" unless result

      true
    end
  end
end
