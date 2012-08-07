# Copyright (c) 2009-2011 WMware, Inc.
require "mongodb_service/job/util"
require "mongodb_service/mongodb_error"

module VCAP::Services::MongoDB::Serialization
  include VCAP::Services::Base::AsyncJob::Serialization

  # Validate the serialized data file.
  def validate_input(files, manifest)
    raise "Doesn't contains any snapshot file." if files.empty?
    raise "Invalide version:#{version}" if manifest[:version] != 1
    true
  end

  class ImportFromURLJob < BaseImportFromURLJob
    include VCAP::Services::MongoDB::Serialization
  end
end

