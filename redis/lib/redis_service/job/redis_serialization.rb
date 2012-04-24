# Copyright (c) 2009-2011 VMware, Inc.
$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..")
require "util"
require "redis_error"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "..")
require "redis_service/redis_node"

module VCAP::Services::Redis::Serialization
  include VCAP::Services::Base::AsyncJob::Serialization

  # Download serialized data from url and import into database
  class ImportFromURLJob < BaseImportFromURLJob
    def snapshot_filename name, snapshot_id
      "dump.rdb"
    end
  end

  # Import serailzed data, which is saved in temp file, into database
  class ImportFromDataJob < BaseImportFromDataJob
    include VCAP::Services::Redis::Util
    include VCAP::Services::Redis::Serialization

    def snapshot_filename name, snapshot_id
      "dump.rdb"
    end
  end
end
