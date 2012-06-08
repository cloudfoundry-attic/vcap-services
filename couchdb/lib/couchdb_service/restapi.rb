# Copyright (c) 2009-2011 VMware, Inc.
require "couchrest"

module VCAP
  module Services
    module CouchDB
      module RestAPI
        def couchdb_add_db(provisioned_service)
          @logger.debug("Creating database: #{provisioned_service.inspect}")

          server = server_admin_connection
          server.database!(provisioned_service.name)

          @logger.debug("Done creating #{provisioned_service.inspect}")
        rescue => e
          @logger.error("Failure creating database #{provisioned_service.name}: #{e.message}")
        end

        def couchdb_delete_db(provisioned_service)
          @logger.debug("Deleting database: #{provisioned_service.inspect}")

          server = server_admin_connection
          server.database(provisioned_service.name).delete!

          @logger.debug("Done deleting #{provisioned_service.inspect}")
        rescue => e
          @logger.error("Failure deleting database #{provisioned_service.name}: #{e.message}")
        end

        def couchdb_add_database_user(provisioned_service, user = nil, password = nil)
          user ||= provisioned_service.user
          password ||= provisioned_service.password

          @logger.info("Creating credentials: #{user}/#{password} for database #{provisioned_service.name}")

          server = server_admin_connection
          couchdb_add_user_credentials(server, provisioned_service.name, user, password)
          couchdb_grant_user_access(server, provisioned_service.name, user)

          @logger.info("Created credentials: #{user}/#{password} for database #{provisioned_service.name}")
        rescue => e
          @logger.error("Failed add user #{user}: #{e.message}")
          raise "Failure creating credentials #{user} for database #{provisioned_service.name}: #{e.message}"
        end

        def couchdb_delete_database_user(provisioned_service, user = nil)
          user ||= provisioned_service.user

          @logger.info("Deleting user #{user}")

          server = server_admin_connection
          couchdb_revoke_user_access(server, provisioned_service.name, user)
          couchdb_delete_user_credentials(server, user)

          @logger.info("Deleted user #{user}")
        rescue => e
          @logger.error("Failure deleting user #{user}: #{e.message}")
        end

        def couchdb_flush_bound_users(provisioned_service)
          server = server_admin_connection
          db = server.database("_users")
          bound_users = couchdb_users_for_db(db, provisioned_service.name)

          bound_users.each do |u|
            couchdb_revoke_user_access(server, provisioned_service.name, u)
            couchdb_delete_user_credentials(server, u)
          end
        end

        def couchdb_overall_stats(provisioned_service)
          server = server_user_connection(provisioned_service)
          CouchRest.get("#{server.uri}/_stats")
        rescue => e
          @logger.error("Failed couchdb_overall_stats: #{e.message}, #{provisioned_service.inspect}")
          "Failed couchdb_overall_stats: #{e.message}, options: #{provisioned_service.inspect}"
        end

        def couchdb_db_stats(provisioned_service)
          server = server_user_connection(provisioned_service)
          CouchRest.get("#{server.uri}/#{provisioned_service.name}")
        rescue => e
          @logger.error("Failed couchdb_db_stats: #{e.message}, options: #{provisioned_service.inspect}")
          "Failed couchdb_db_stats: #{e.message}, options: #{provisioned_service.inspect}"
        end

        def get_healthz(provisioned_service)
          server = server_user_connection(provisioned_service)
          auth = CouchRest.get("#{server.uri}/#{provisioned_service.name}")
          auth ? "ok" : "fail"
        rescue => e
          "fail"
        end

      protected
        def couchdb_add_user_credentials(server, name, user, password)
          auth_db = server.database("_users")

          key = security_key(user)
          salt = generate_salt

          doc = {"_id" => key,
                  "type" => "user",
                  "name" => user,
                  "roles" => ["whatever"],
                  "password_sha" => Digest::SHA1.hexdigest("#{password}#{salt}"),
                  "salt" => salt,
                  "bind_db" => name}

          begin
            d = auth_db.get(key)
            d.merge!(doc)
            d.save
          rescue RestClient::ResourceNotFound
            auth_db_url = "#{server.uri}#{auth_db.uri}"
            RestClient.post(auth_db_url, doc.to_json, {"Content-Type" => "application/json"})
          end
        end

        def couchdb_delete_user_credentials(server, user)
          auth_db = server.database("_users")
          doc = auth_db.get(security_key(user))
          doc.destroy
        end

        def couchdb_grant_user_access(server, name, user)
          db = server.database(name)
          rights = db.get("_security")
          security_defaults!(rights)

          rights["admins"]["names"] << user unless rights["admins"]["names"].include?(user)
          rights["readers"]["names"] << user unless rights["readers"]["names"].include?(user)

          RestClient.put("#{server.uri}#{db.uri}/_security", rights.to_json, {"Content-Type" => "application/json"})
        end

        def couchdb_revoke_user_access(server, name, user)
          db = server.database(name)
          rights = db.get("_security")
          security_defaults!(rights)

          rights["admins"]["names"].delete(user)
          rights["readers"]["names"].delete(user)
          RestClient.put("#{server.uri}#{db.uri}/_security", rights.to_json, {"Content-Type" => "application/json"})
        end

        def couchdb_users_for_db(db, name)
          rows = db.documents(:include_docs => true)["rows"]
          rows.select { |u| u["doc"]["type"] == "user" && u["doc"]["bind_db"] == name }.map { |u| u["doc"]["name"] }
        end
      end
    end
  end
end
