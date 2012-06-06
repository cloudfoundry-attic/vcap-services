# Copyright (c) 2009-2011 VMware, Inc.
module VCAP
  module Services
    module CouchDB

      # FIXME this should probably go into common
      module Util
        def _server_connection(host, port, username, password)
          url = "http://"
          if username
            url << username
            if password
              url << ":"
              url << password
            end
            url << "@"
          end
          url << "#{host}:#{port}"
          conn = CouchRest.new(url)
        end

        def server_user_connection(provisioned_service)
          _server_connection(@couchdb_config['host'], provisioned_service.port,
                              provisioned_service.user, provisioned_service.password)
        end

        def server_admin_connection
          _server_connection(@couchdb_config['host'], @couchdb_config['port'],
                            @couchdb_config['admin'], @couchdb_config['adminpass'])
        end

        def security_key(user)
          "org.couchdb.user:#{user}"
        end

        def security_defaults!(rights)
          rights["admins"] ||= {}
          rights["admins"]["names"] ||= []
          rights["admins"]["roles"] ||= []
          rights["readers"] ||= {}
          rights["readers"]["names"] ||= []
          rights["readers"]["roles"] ||= []
        end
      end
    end
  end
end
