# Copyright (c) 2009-2011 VMware, Inc.
$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..")
require "util"
require "redis_error"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "..")
require "redis_service/redis_node"

module VCAP::Services::Redis::Serialization
  include VCAP::Services::Base::AsyncJob::Serialization

  def init_localdb(database_url)
    DataMapper.setup(:default, database_url)
  end

  def init_command_name(prefix)
    @config_command_name = prefix + "-config"
    @shutdown_command_name = prefix + "-shutdown"
    @save_command_name = prefix + "-save"
  end

  def redis_provisioned_service
    VCAP::Services::Redis::Node::ProvisionedService
  end


  # Dump a database into files just as create snapshot job.
  # Create a download token in redis so user is able to download the serialzed data.
  class CreateSerializedURLJob < BaseCreateSerializedURLJob
    include VCAP::Services::Redis::Util
    include VCAP::Services::Redis::Serialization

    def execute
      init_localdb(@config["local_db"])
      init_command_name(@config["command_rename_prefix"])

      dump_path = get_serialized_data_path(name)
      # Clean up previous data
      cleanup(name)
      FileUtils.mkdir_p(dump_path)
      dump_file_name = "#{name}.gz"

      srv = redis_provisioned_service.get(name)
      result = dump_redis_data(srv, dump_path, @config["gzip_bin"], dump_file_name)
      raise "Failed to execute dump command to #{name}" unless result

      {:dump_file_name => dump_file_name}
    end
  end

  # Download serialized data from url and import into database
  class ImportFromURLJob < BaseImportFromURLJob
    include VCAP::Services::Redis::Util
    include VCAP::Services::Redis::Serialization

    def execute
      init_localdb(@config["local_db"])
      init_command_name(@config["command_rename_prefix"])

      srv = redis_provisioned_service.get(name)
      tmp_file_name = File.basename @temp_file_path
      result = import_redis_data(srv, @config["tmp_dir"], @config["base_dir"], @config["redis_server_path"], @config["gzip_bin"], tmp_file_name)
      raise "Failed to execute import command to #{name}" unless result
      srv.pid = result
      srv.save

      true
    end
  end

  # Import serailzed data, which is saved in temp file, into database
  class ImportFromDataJob < BaseImportFromDataJob
    include VCAP::Services::Redis::Util
    include VCAP::Services::Redis::Serialization

    def execute
      init_localdb(@config["local_db"])
      init_command_name(@config["command_rename_prefix"])

      srv = redis_provisioned_service.get(name)
      tmp_file_name = File.basename @temp_file_path
      result = import_redis_data(srv, @config["tmp_dir"], @config["base_dir"], @config["redis_server_path"], @config["gzip_bin"], tmp_file_name)
      raise "Failed to execute import command to #{name}" unless result
      srv.pid = result
      srv.save

      true
    end
  end
end
