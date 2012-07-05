# Copyright (c) 2009-2011 VMware, Inc.
module VCAP
  module Services
    module Echo
      class EchoError < VCAP::Services::Base::Error::ServiceError
        ECHO_SAVE_INSTANCE_FAILED        = [32100, HTTP_INTERNAL, "Could not save instance: %s"]
        ECHO_DESTORY_INSTANCE_FAILED     = [32101, HTTP_INTERNAL, "Could not destroy instance: %s"]
        ECHO_FIND_INSTANCE_FAILED        = [32102, HTTP_NOT_FOUND, "Could not find instance: %s"]
        ECHO_START_INSTANCE_FAILED       = [32103, HTTP_INTERNAL, "Could not start instance: %s"]
        ECHO_STOP_INSTANCE_FAILED        = [32104, HTTP_INTERNAL, "Could not stop instance: %s"]
        ECHO_INVALID_PLAN                = [32105, HTTP_INTERNAL, "Invalid plan: %s"]
        ECHO_CLEANUP_INSTANCE_FAILED     = [32106, HTTP_INTERNAL, "Could not cleanup instance, the reasons: %s"]
      end
    end
  end
end
