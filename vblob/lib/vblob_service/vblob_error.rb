# Copyright (c) 2009-2011 VMware, Inc.

module VCAP
  module Services
    module VBlob
      class VBlobError < VCAP::Services::Base::Error::ServiceError
        VBLOB_DISK_FULL = [31601, HTTP_INTERNAL, 'Node disk is full.']
        VBLOB_CONFIG_NOT_FOUND = [31602, HTTP_NOT_FOUND, 'vBlob configuration %s not found.']
        VBLOB_CRED_NOT_FOUND = [31603, HTTP_NOT_FOUND, 'vBlob credential %s not found.']
        VBLOB_INVALID_PLAN = [31604, HTTP_INTERNAL, 'vBlob plan %s.']
        VBLOB_START_INSTANCE_ERROR = [31605, HTTP_INTERNAL, 'vBlob start instance failed']
        VBLOB_PROVISION_ERROR = [31606, HTTP_INTERNAL, 'vBlob provision failed']
        VBLOB_CLEANUP_ERROR = [31607, HTTP_INTERNAL, 'vBlob cleanup failed: %s']
        VBLOB_ADD_USER_ERROR = [31608, HTTP_INTERNAL, 'vBlob add user failed: %s']
        VBLOB_REMOVE_USER_ERROR = [31609, HTTP_INTERNAL, 'vBlob remove user failed: %s']
      end
    end
  end
end
