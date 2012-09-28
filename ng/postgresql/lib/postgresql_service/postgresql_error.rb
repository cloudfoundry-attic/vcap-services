# Copyright (c) 2009-2011 VMware, Inc.

class VCAP::Services::Postgresql::PostgresqlError<
  VCAP::Services::Base::Error::ServiceError
    POSTGRESQL_DISK_FULL = [31801, HTTP_INTERNAL, 'Node disk is full.']
    POSTGRESQL_CONFIG_NOT_FOUND = [31802, HTTP_NOT_FOUND, 'Postgresql configuration %s not found.']
    POSTGRESQL_CRED_NOT_FOUND = [31803, HTTP_NOT_FOUND, 'Postgresql credential %s not found.']
    POSTGRESQL_LOCAL_DB_ERROR = [31804, HTTP_INTERNAL, 'Postgresql node local db error.']
    POSTGRESQL_INVALID_PLAN = [31805, HTTP_INTERNAL, 'Invalid plan %s.']
    POSTGRESQL_DB_ERROR = [31806, HTTP_INTERNAL, 'Postgresql node database error.']
    POSTGRESQL_BAD_SERIALIZED_DATA = [31807, HTTP_INTERNAL, 'Invalid serialized data.']
end
