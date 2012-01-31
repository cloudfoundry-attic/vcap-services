# Copyright (c) 2009-2011 VMware, Inc.

module VCAP
  module Services
    module Rabbit
      class RabbitError < VCAP::Services::Base::Error::ServiceError
        # 31300 - 31399  Rabbit-specific Error
        RABBIT_SAVE_INSTANCE_FAILED     = [31300, HTTP_INTERNAL, "Could not save instance: %s"]
        RABBIT_DESTORY_INSTANCE_FAILED  = [31301, HTTP_INTERNAL, "Could not destroy instance: %s"]
        RABBIT_FIND_INSTANCE_FAILED     = [31302, HTTP_NOT_FOUND, "Could not find instance: %s"]
        RABBIT_START_INSTANCE_FAILED    = [31303, HTTP_INTERNAL, "Could not start instance: %s"]
        RABBIT_STOP_INSTANCE_FAILED     = [31304, HTTP_INTERNAL, "Could not stop instance: %s"]
        RABBIT_CLEANUP_INSTANCE_FAILED  = [31305, HTTP_INTERNAL, "Could not cleanup instance, the reasons: %s"]
        RABBIT_INVALID_PLAN             = [31306, HTTP_INTERNAL, "Invalid plan: %s"]
        RABBIT_START_SERVER_FAILED      = [31307, HTTP_INTERNAL, "Could not start rabbitmq server"]
        RABBIT_STOP_SERVER_FAILED       = [31308, HTTP_INTERNAL, "Could not stop rabbitmq server"]
        RABBIT_ADD_VHOST_FAILED         = [31309, HTTP_INTERNAL, "Could not add vhost: %s"]
        RABBIT_DELETE_VHOST_FAILED      = [31310, HTTP_INTERNAL, "Could not delete vhost: %s"]
        RABBIT_ADD_USER_FAILED          = [31311, HTTP_INTERNAL, "Could not add user: %s"]
        RABBIT_DELETE_USER_FAILED       = [31312, HTTP_INTERNAL, "Could not delete user: %s"]
        RABBIT_GET_PERMISSIONS_FAILED   = [31313, HTTP_INTERNAL, "Could not get user %s permission"]
        RABBIT_SET_PERMISSIONS_FAILED   = [31314, HTTP_INTERNAL, "Could not set user %s permission to %s"]
        RABBIT_CLEAR_PERMISSIONS_FAILED = [31315, HTTP_INTERNAL, "Could not clean user %s permissions"]
        RABBIT_LIST_USERS_FAILED        = [31316, HTTP_INTERNAL, "Could not list users"]
        RABBIT_LIST_QUEUES_FAILED       = [31317, HTTP_INTERNAL, "Could not list queues on vhost %s"]
        RABBIT_LIST_EXCHANGES_FAILED    = [31318, HTTP_INTERNAL, "Could not list exchanges on vhost %s"]
        RABBIT_LIST_BINDINGS_FAILED     = [31319, HTTP_INTERNAL, "Could not list bindings on vhost %s"]
      end
    end
  end
end
