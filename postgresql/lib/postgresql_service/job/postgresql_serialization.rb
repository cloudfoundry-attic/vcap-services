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

  class ImportFromURLJob < BaseImportFromURLJob
    include VCAP::Services::Postgresql::Serialization

    def snapshot_filename name, snapshot_id
      "#{snapshot_id}.dump"
    end
  end

  class ImportFromDataJob < BaseImportFromDataJob
    include VCAP::Services::Postgresql::Serialization

    def snapshot_filename name, snapshot_id
      "#{snapshot_id}.dump"
    end
  end
end
