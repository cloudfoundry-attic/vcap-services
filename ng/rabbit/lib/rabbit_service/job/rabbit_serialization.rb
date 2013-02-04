# Copyright (c) 2009-2011 VMware, Inc.
$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..')
require "util"
require "rabbit_error"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..')
require "rabbit_service/rabbit_node"

module VCAP::Services::Rabbit::Serialization
  include VCAP::Services::Base::AsyncJob::Serialization

  # Validate the serialized data file.
  def validate_input(files, manifest)
    raise "Doesn't contains any snapshot file." if files.empty?
    raise "Invalide version:#{version}" if manifest[:version] != 1
    true
  end

  def rabbit_provisioned_service
    VCAP::Services::Rabbit::Node::ProvisionedService
  end

  class ImportFromURLJob < BaseImportFromURLJob
    include VCAP::Services::Rabbit::Serialization
  end
end
