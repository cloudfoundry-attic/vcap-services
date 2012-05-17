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

  def mysql_provisioned_service
    VCAP::Services::Mysql::Node::ProvisionedService
  end

  class ImportFromURLJob < BaseImportFromURLJob
    include VCAP::Services::Mysql::Serialization

    def snapshot_filename name, snapshot_id
      "#{snapshot_id}.sql.gz"
    end
  end

  class ImportFromDataJob < BaseImportFromDataJob
    include VCAP::Services::Mysql::Serialization

    def snapshot_filename name, snapshot_id
      "#{snapshot_id}.sql.gz"
    end
  end
end
