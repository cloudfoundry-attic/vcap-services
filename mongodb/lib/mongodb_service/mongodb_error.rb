# Copyright (c) 2009-2011 VMware, Inc.

class VCAP::Services::MongoDB::MongoDBError < VCAP::Services::Base::Error::ServiceError
    MONGODB_DISK_FULL = [31201, HTTP_INTERNAL, 'Node disk is full.']
    MONGODB_CONFIG_NOT_FOUND = [31202, HTTP_NOT_FOUND, 'MongoDB configuration %s not found.']
    MONGODB_CRED_NOT_FOUND = [31203, HTTP_NOT_FOUND, 'MongoDB credential %s not found.']
    MONGODB_LOCAL_DB_ERROR = [31204, HTTP_INTERNAL, 'MongoDB node local db error.']
    MONGODB_INVALID_PLAN = [31205, HTTP_INTERNAL, 'Invalid plan %s.']
    MONGODB_BAD_SERIALIZED_DATA = [31207, HTTP_BAD_REQUEST, "File %s can't be verified"]
end
