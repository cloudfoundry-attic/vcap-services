# Copyright (c) 2009-2011 VMware, Inc.
# This code is based on Redis as a Service.

module VCAP
  module Services
    module CouchDB
      class CouchDbError < VCAP::Services::Base::Error::ServiceError
        COUCHDB_INVALID_PLAN            = [32200, HTTP_INTERNAL, "Invalid plan: %s"]
        COUCHDB_SAVE_INSTANCE_FAILED    = [32201, HTTP_INTERNAL, "Failed to save service instance: %s"]
        COUCHDB_CLEANUP_INSTANCE_FAILED = [32202, HTTP_INTERNAL, "Failed to cleanup service instance: %s"]
      end
    end
  end
end
