# Copyright (c) 2009-2011 VMware, Inc.
$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..")
require "util"
require "redis_error"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "..")
require "redis_service/redis_node"

module VCAP::Services::Redis::Serialization
  include VCAP::Services::Base::AsyncJob::Serialization

  # Validate the serialized data file.
  def validate_input(files, manifest)
    raise "Doesn't contains any snapshot file." if files.empty?
    raise "Invalide version:#{version}" if manifest[:version] != 1
    true
  end

  # Download serialized data from url and import into database
  class ImportFromURLJob < BaseImportFromURLJob
    include VCAP::Services::Redis::Serialization
  end
end
