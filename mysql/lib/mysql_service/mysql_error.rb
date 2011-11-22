# Copyright (c) 2009-2011 VMware, Inc.

class VCAP::Services::Mysql::MysqlError<
  VCAP::Services::Base::Error::ServiceError
    MYSQL_DISK_FULL = [31001, HTTP_INTERNAL, 'Node disk is full.']
    MYSQL_CONFIG_NOT_FOUND = [31002, HTTP_NOT_FOUND, 'Mysql configuration %s not found.']
    MYSQL_CRED_NOT_FOUND = [31003, HTTP_NOT_FOUND, 'Mysql credential %s not found.']
    MYSQL_LOCAL_DB_ERROR = [31004, HTTP_INTERNAL, 'Mysql node local db error.']
    MYSQL_INVALID_PLAN = [31005, HTTP_INTERNAL, 'Invalid plan %s.']
    MYSQL_BAD_SERIALIZED_DATA = [31007, HTTP_BAD_REQUEST, "File %s can't be verified"]
end
