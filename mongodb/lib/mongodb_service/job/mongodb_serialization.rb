# Copyright (c) 2009-2011 WMware, Inc.
require "mongodb_service/job/util"
require "mongodb_service/mongodb_error"

module VCAP::Services::MongoDB::Serialization
  include VCAP::Services::Base::AsyncJob::Serialization

  class ImportFromURLJob < BaseImportFromURLJob
    def snapshot_filename name, snapshot_id
      "#{snapshot_id}.tgz"
    end
  end

  class ImportFromDataJob < BaseImportFromDataJob
    def snapshot_filename name, snapshot_id
      "#{snapshot_id}.tgz"
    end
  end

end

