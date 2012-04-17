# Copyright (c) 2009-2011 WMware, Inc.
require "mongodb_service/job/util"
require "mongodb_service/mongodb_error"

module VCAP::Services::MongoDB::Serialization
  include VCAP::Services::Base::AsyncJob::Serialization

  # Dump a database into files just as create snapshot job.
  # Create a download token in redis so user is able to download the serialized data.
  class CreateSerializedURLJob < BaseCreateSerializedURLJob
    include VCAP::Services::MongoDB::Util

    def execute
      dump_path = get_serialized_data_path(name)
      cleanup(name)
      FileUtils.mkdir_p(dump_path)

      dump_file_name = "#{name}.gz"
      dump_file_path = File.join(dump_path, dump_file_name)

      result = dump_database(name, dump_file_path)
      raise "Failed to execute dump command to #{name}" unless result

      {:dump_file_name => dump_file_name}
    end
  end

  class ImportFromURLJob < BaseImportFromURLJob
    include VCAP::Services::MongoDB::Util

    def execute
      DataMapper.setup(:default, @config['local_db'])
      DataMapper::auto_upgrade!

      result = restore_database(name, temp_file_path)
      raise "Failed to execute import command to #{name}" unless result

      true
    end
  end

  # Import serailzed data, which is saved in temp file, into database
  class ImportFromDataJob < BaseImportFromDataJob
    include VCAP::Services::MongoDB::Util

    def execute
      DataMapper.setup(:default, @config['local_db'])
      DataMapper::auto_upgrade!

      result = restore_database(name, temp_file_path)
      raise "Failed to execute import command to #{name}" unless result

      true
    end
  end

end

