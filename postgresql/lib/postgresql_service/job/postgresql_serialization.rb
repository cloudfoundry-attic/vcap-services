# Copyright (c) 2009-2011 VMware, Inc.

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..')
require "util"
require "postgresql_error"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..')
require "postgresql_service/node"

module VCAP::Services::Postgresql::Serialization
  include VCAP::Services::Base::AsyncJob::Serialization

  # Validate the serialized data file.
  def validate_input(files, manifest)
    raise "Doesn't contains any snapshot file." if files.empty?
    raise "Invalide version:#{version}" if manifest[:version] != 1
    true
  end

  class ImportFromURLJob < BaseImportFromURLJob
    include VCAP::Services::Postgresql::Serialization
  end
end
