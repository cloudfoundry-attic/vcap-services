# Copyright (c) 2009-2011 VMware, Inc.

module VCAP
  module Services
    module Rabbit
      class RabbitmqError < VCAP::Services::Base::Error::ServiceError
        # 31300 - 31399  Rabbit-specific Error
        RABBITMQ_SAVE_INSTANCE_FAILED         = [31300, HTTP_INTERNAL, "Could not save instance: %s"]
        RABBITMQ_DESTORY_INSTANCE_FAILED      = [31301, HTTP_INTERNAL, "Could not destroy instance: %s"]
        RABBITMQ_FIND_INSTANCE_FAILED         = [31302, HTTP_NOT_FOUND, "Could not find instance: %s"]
        RABBITMQ_START_INSTANCE_FAILED        = [31303, HTTP_INTERNAL, "Could not start instance: %s"]
        RABBITMQ_STOP_INSTANCE_FAILED         = [31304, HTTP_INTERNAL, "Could not stop instance: %s"]
        RABBITMQ_CLEANUP_INSTANCE_FAILED      = [31305, HTTP_INTERNAL, "Could not cleanup instance, the reasons: %s"]
        RABBITMQ_INVALID_PLAN                 = [31306, HTTP_INTERNAL, "Invalid plan: %s"]
        RABBITMQ_START_SERVER_FAILED          = [31307, HTTP_INTERNAL, "Could not start rabbitmq server"]
        RABBITMQ_STOP_SERVER_FAILED           = [31308, HTTP_INTERNAL, "Could not stop rabbitmq server"]
        RABBITMQ_ADD_VHOST_FAILED             = [31309, HTTP_INTERNAL, "Could not add vhost: %s"]
        RABBITMQ_DELETE_VHOST_FAILED          = [31310, HTTP_INTERNAL, "Could not delete vhost: %s"]
        RABBITMQ_ADD_USER_FAILED              = [31311, HTTP_INTERNAL, "Could not add user: %s"]
        RABBITMQ_DELETE_USER_FAILED           = [31312, HTTP_INTERNAL, "Could not delete user: %s"]
        RABBITMQ_GET_PERMISSIONS_FAILED       = [31313, HTTP_INTERNAL, "Could not get user %s permission"]
        RABBITMQ_SET_PERMISSIONS_FAILED       = [31314, HTTP_INTERNAL, "Could not set user %s permission to %s"]
        RABBITMQ_CLEAR_PERMISSIONS_FAILED     = [31315, HTTP_INTERNAL, "Could not clean user %s permissions"]
        RABBITMQ_GET_VHOST_PERMISSIONS_FAILED = [31316, HTTP_INTERNAL, "Could not get vhost %s permissions"]
        RABBITMQ_LIST_USERS_FAILED            = [31317, HTTP_INTERNAL, "Could not list users"]
        RABBITMQ_LIST_QUEUES_FAILED           = [31318, HTTP_INTERNAL, "Could not list queues on vhost %s"]
        RABBITMQ_LIST_EXCHANGES_FAILED        = [31319, HTTP_INTERNAL, "Could not list exchanges on vhost %s"]
        RABBITMQ_LIST_BINDINGS_FAILED         = [31320, HTTP_INTERNAL, "Could not list bindings on vhost %s"]
        RABBITMQ_RUN_SYSTEM_COMMAND_FAILED    = [31321, HTTP_INTERNAL, "Failed to run system command \"%s\", stdout: %s, stderr: %s"]
        RABBITMQ_START_INSTANCE_TIMEOUT       = [31322, HTTP_INTERNAL, "Timeout to start RabbitMQ server for instance %s"]

      end
    end
  end
end
