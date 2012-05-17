# Copyright (c) 2009-2011 VMware, Inc.

class VCAP::Services::Postgresql::PostgresqlError<
  VCAP::Services::Base::Error::ServiceError
    POSTGRESQL_DISK_FULL = [32001, HTTP_INTERNAL, 'Node disk is full.']
    POSTGRESQL_CONFIG_NOT_FOUND = [32002, HTTP_NOT_FOUND, 'Postgresql configuration %s not found.']
    POSTGRESQL_CRED_NOT_FOUND = [32003, HTTP_NOT_FOUND, 'Postgresql credential %s not found.']
    POSTGRESQL_LOCAL_DB_ERROR = [32004, HTTP_INTERNAL, 'Postgresql node local db error.']
    POSTGRESQL_INVALID_PLAN = [32005, HTTP_INTERNAL, 'Invalid plan %s.']
    POSTGRESQL_BAD_SERIALIZED_DATA = [32007, HTTP_INTERNAL, 'Invalid serialized data.']
end
