# Copyright (c) 2009-2011 VMware, Inc.
require "rest_client"
require "json"

module VCAP
  module Services
    module Rabbit
      module Util
        @rabbitmq_timeout ||= 2

        def create_resource(credentials)
          RestClient::Resource.new("http://#{credentials["username"]}:#{credentials["password"]}@#{credentials["hostname"]}:#{service_admin_port}/api", :timeout => @rabbitmq_timeout)
        end

        def add_vhost(credentials, vhost)
          response = create_resource(credentials)["vhosts/#{vhost}"].put nil, :content_type => "application/json"
          raise RabbitmqError.new(RabbitmqError::RABBITMQ_ADD_VHOST_FAILED, vhost) if response != ""
        end

        def delete_vhost(credentials, vhost)
          response = create_resource(credentials)["vhosts/#{vhost}"].delete
          raise RabbitmqError.new(RabbitmqError::RABBITMQ_DELETE_VHOST_FAILED, vhost) if response != ""
        end

        def add_user(credentials, username, password, tags="administrator")
          response = create_resource(credentials)["users/#{username}"].put "{\"password\":\"#{password}\", \"tags\":\"#{tags}\"}", :content_type => "application/json"
          raise RabbitmqError.new(RabbitmqError::RABBITMQ_ADD_USER_FAILED, username) if response != ""
        end

        def delete_user(credentials, username)
          response = create_resource(credentials)["users/#{username}"].delete
          raise RabbitmqError.new(RabbitmqError::RABBITMQ_DELETE_USER_FAILED, username) if response != ""
        end

        def get_permissions_by_options(binding_options)
          # FIXME: binding options is not implemented, use the full permissions.
          '{"configure":".*","write":".*","read":".*"}'
        end

        def get_permissions(credentials, vhost, username)
          response = create_resource(credentials)["permissions/#{vhost}/#{username}"].get
          JSON.parse(response)
        end

        def set_permissions(credentials, vhost, username, permissions)
          response = create_resource(credentials)["permissions/#{vhost}/#{username}"].put permissions, :content_type => "application/json"
          raise RabbitmqError.new(RabbitmqError::RABBITMQ_SET_PERMISSIONS_FAILED, username, permissions) if response != ""
        end

        def clear_permissions(credentials, vhost, username)
          response = create_resource(credentials)["permissions/#{vhost}/#{username}"].delete
          raise RabbitmqError.new(RabbitmqError::RABBITMQ_CLEAR_PERMISSIONS_FAILED, username) if response != ""
        end

        def get_vhost_permissions(credentials, vhost)
          response = create_resource(credentials)["vhosts/#{vhost}/permissions"].get
          JSON.parse(response)
        end

        def list_users(credentials)
          response = create_resource(credentials)["users"].get
          JSON.parse(response)
        end

        def list_queues(credentials, vhost)
          response = create_resource(credentials)["queues"].get
          JSON.parse(response)
        end

        def list_exchanges(credentials, vhost)
          response = create_resource(credentials)["exchanges"].get
          JSON.parse(response)
        end

        def list_bindings(credentials, vhost)
          response = create_resource(credentials)["bindings"].get
          JSON.parse(response)
        end

      end
    end
  end
end
