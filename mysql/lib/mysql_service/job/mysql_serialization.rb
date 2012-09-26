# Copyright (c) 2009-2011 VMware, Inc.
$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..')
require "util"
require "mysql_error"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..')
require "mysql_service/node"

module VCAP::Services::Mysql::Serialization
  include VCAP::Services::Base::AsyncJob::Serialization

  # Validate the serialized data file.
  def validate_input(files, manifest)
    raise "Doesn't contains any snapshot file." if files.empty?
    raise "Invalide version:#{version}" if manifest[:version] != 1
    true
  end

  def mysql_provisioned_service
    VCAP::Services::Mysql::Node::ProvisionedService
  end

  class ImportFromURLJob < BaseImportFromURLJob
    include VCAP::Services::Mysql::Serialization
  end
end
