# Copyright (c) 2009-2011 VMware, Inc.
require 'tempfile'
require 'fileutils'
require 'open3'
require 'postgresql_service/pg_timeout'
require 'postgresql_service/pg_version'

module VCAP
  module Services
    module Postgresql

      # Give various helper functions
      module Util
        VALID_CREDENTIAL_CHARACTERS = ("A".."Z").to_a + ("a".."z").to_a + ("0".."9").to_a

        include VCAP::Services::Base::Utils
        include VCAP::Services::Postgresql::Version

        def fmt_error(e)
          "#{e}: [#{e.backtrace.join(" | ")}]"
        end

        def ignore_exception
          begin
            yield
          rescue => e
          end
        end

        def create_logger(logdev=STDOUT, rotation=0, level=Logger::DEBUG)
          if String === logdev
            dir = File.dirname(logdev)
            FileUtils.mkdir_p(dir) unless File.directory?(dir)
          end
          logger = Logger.new(logdev, rotation)
          logger.level = case level
            when "DEBUG" then Logger::DEBUG
            when "INFO"  then Logger::INFO
            when "WARN"  then Logger::WARN
            when "ERROR" then Logger::ERROR
            when "FATAL" then Logger::FATAL
            else Logger::UNKNOWN
          end
          logger
        end

        # shell CMD wrapper and logger
        def exe_cmd(cmd, env={}, stdin=nil)
          @logger ||= create_logger
          @logger.debug("Execute shell cmd:[#{env}, '#{cmd}']")
          o, e, s = Open3.capture3(env, cmd, :stdin_data => stdin)
          if s.exitstatus == 0
            @logger.info("Execute cmd:[#{cmd}] succeeded.")
          else
            @logger.error("Execute cmd:[#{cmd}] failed. Stdin:[#{stdin}], stdout: [#{o}], stderr:[#{e}]")
          end
          return [o, e, s]
        end

        def postgresql_connect(host, user, password, port, database, opts={})
          quick_mode = opts[:quick] || false
          opts.merge!(
            :fail_with_nil => true,
            :connect_timeout => 3,
            :try_num => 1,
            :exception_sleep => 0,
            :quiet => true,
          ) if quick_mode

          fail_with_nil = opts[:fail_with_nil] || true
          connect_timeout = opts[:connect_timeout]
          try_num = opts[:try_num] || 5
          exception_sleep = opts[:exception_sleep] || 1
          quiet = opts[:quiet] || false

          @logger ||= create_logger
          conn_opts = {
            :host => host,
            :port => port,
            :options => nil,
            :tty => nil,
            :dbname => database,
            :user => user,
            :password => password
          }

          # if connect_timeout not set, will use PGDBconn.default_connect_timeout
          # see postgresql_service/pg_timeout
          conn_opts.merge!(:connect_timeout => connect_timeout) if connect_timeout

          try_num.times do
            begin
              @logger.info("PostgreSQL connect: #{host}, #{port}, #{user}, #{password || "***"}, #{database} (fail_with_nil: #{fail_with_nil})") unless quiet
              conn = PGDBconn.new(conn_opts)
              version = pg_version(conn, :full => true)
              @logger.info("Connected PostgreSQL server - version: #{version}") unless quiet
              return conn
            rescue => e
              @logger.error("PostgreSQL connection attempt failed: #{host} #{port} #{database} #{user} #{password || "***"}") unless quiet
              sleep(exception_sleep) if exception_sleep > 0
            end
          end

          if fail_with_nil
            @logger.warn("PostgreSQL connection unrecoverable") unless quiet
            return nil
          else
            @logger.fatal("PostgreSQL connection unrecoverable")
            shutdown if self.respond_to?(:shutdown)
            exit
          end
        end

        # Return the public schema id of postgresql
        def get_public_schema_id(conn)
          schema_id = nil
          if conn
            res = conn.query("select oid, nspname, nspowner from pg_namespace where nspname = 'public'")
            res.each do |nsp|
              schema_id = nsp['oid']
              break
            end
          end
          schema_id
        end

        # Return all schemas owned by current logined user
        def get_conn_schemas(default_connection)
          if default_connection
            schemas = {}
            res = default_connection.query("select n.oid as nspid,n.nspname,n.nspowner from pg_namespace as n inner join pg_roles as r on n.nspowner = r.oid where r.rolname = '#{default_connection.user}'")
            res.each do |ns|
              schemas[ns['nspname']] = ns['nspid']
            end
            schemas
          else
            nil
          end
        end

        # Legacy method to alter owner of relationship from sys_user to user
        def do_grant_query(db_connection,user,sys_user)
          return unless db_connection
          db_connection.query("update pg_class set relowner = (select oid from pg_roles where rolname = '#{user}') where relowner = (select oid from pg_roles where rolname = '#{sys_user}')")
        end

        # Legacy method to revoke privileges of public shcema
        def do_revoke_query(db_connection, user, sys_user)
          db_connection.query("revoke create on schema public from #{user} CASCADE")
          db_connection.query("REVOKE ALL ON ALL TABLES IN SCHEMA PUBLIC from #{user} CASCADE")
          db_connection.query("REVOKE ALL ON ALL SEQUENCES IN SCHEMA PUBLIC from #{user} CASCADE")
          db_connection.query("REVOKE ALL ON ALL FUNCTIONS IN SCHEMA PUBLIC from #{user} CASCADE")

          # with the fix for user access rights in r8, actually this line is a no-op.
          # - for newly created users(after the fix), all objects created will be owned by parent
          # - for existing users(created before the fix), if quota exceeds, then sys_user will
          #  own the objects, but, when the fix comes, the migration job will pull all the objects
          #  (both user and sys_user) to parent as the owner. So, after the fix comes, there is no
          #  object owned by sys_user.
          # while quota can be still enforced because 'revoke_write_access' and 'do_revoke_query'
          # do the work.
          db_connection.query("update pg_class set relowner = (select oid from pg_roles where rolname = '#{sys_user}') where relowner = (select oid from pg_roles where rolname ='#{user}')")
        end

        # Legacy method to grant user privileges of public schema
        def exe_grant_user_priv(conn)
          @logger ||= create_logger
          unless conn
            @logger.error("No connection to do exe_grant_user_priv")
            return
          end
          conn.query("grant create on schema public to public")
          conn.query("grant all on all tables in schema public to public")
          conn.query("grant all on all sequences in schema public to public")
          conn.query("grant all on all functions in schema public to public")
        end

        # Grant write access privileges to role on schema
        def grant_schema_write_access(db_connection, schema_id, schema, role)
          return unless db_connection
          db_connection.query("grant create on schema #{schema} to #{role}")
          db_connection.query("grant all on all tables in schema #{schema} to #{role}")
          db_connection.query("grant all on all sequences in schema #{schema} to #{role}")
          db_connection.query("grant all on all functions in schema #{schema} to #{role}")
        end

        # Revoke write access privileges from role on schema
        def revoke_schema_write_access(db_connection, schema_id, schema, role)
          return unless db_connection
          db_connection.query("revoke create on schema #{schema} from #{role} CASCADE")
          db_connection.query("REVOKE ALL ON ALL TABLES IN SCHEMA #{schema} from #{role} CASCADE")
          db_connection.query("REVOKE ALL ON ALL SEQUENCES IN SCHEMA #{schema} from #{role} CASCADE")
          db_connection.query("REVOKE ALL ON ALL FUNCTIONS IN SCHEMA #{schema} from #{role} CASCADE")
          db_connection.query("grant select,delete,truncate,references,trigger on all tables in schema #{schema} to #{role}")
          db_connection.query("grant usage,select on all sequences in schema #{schema} to #{role}")
        end

        # Grant write access privilege of database
        def grant_write_access_internal(db_connection, service, public_schema_id=nil)
          return false unless db_connection && service
          @logger ||= create_logger
          name = service.name
          default_user = service.default_user
          unless default_user
            @logger.error("No default user #{default_user} for database #{name} when granting write access")
            return false
          end
          default_connection = postgresql_connect(
                                 db_connection.host,
                                 default_user[:user],
                                 default_user[:password],
                                 db_connection.port,
                                 name, :quick => true)
          unless default_connection
            @logger.error("Default user failed to connect to database #{name} when granting write access")
            return false
          end

          schemas = get_conn_schemas(default_connection) || {}
          db_connection.transaction do |db_conn|
            public_schema_id ||= get_public_schema_id(db_conn)
            unless public_schema_id
              raise "Fail to get public schema id"
            end
            service.pgbindusers.all.each do |binduser|
              user = binduser.user
              sys_user = binduser.sys_user
              sys_password = binduser.sys_password
              db_conn_sys_user = postgresql_connect(db_conn.host, sys_user, sys_password, db_conn.port, name, :quick => true)
              if db_conn_sys_user.nil?
                raise "Unable to grant write access to #{name} for #{sys_user}"
              else
                db_conn_sys_user.close
                do_grant_query(db_conn, user, sys_user)
              end
              db_conn.query("GRANT TEMP ON DATABASE #{name} to #{user}")
              db_conn.query("GRANT TEMP ON DATABASE #{name} to #{sys_user}")
            end
            grant_schema_write_access(db_conn, public_schema_id, 'public', 'public')
            schemas.each do |sc, sc_id|
              grant_schema_write_access(db_conn, sc_id, sc, default_user[:user])
            end
            db_conn.query("grant create on database #{name} to #{default_user[:user]}")
          end
          service.quota_exceeded = false
          service.save
          return true
        rescue => e
          @logger.error("Fail to regrant write access of service #{service.name}: " + fmt_error(e))
          return false
        ensure
          default_connection.close if default_connection
        end

        # Revoke write access privileges of database
        def revoke_write_access_internal(pgconn, db_connection, service, public_schema_id=nil)
          return false unless pgconn && db_connection && service
          @logger ||= create_logger
          name = service.name
          default_user = service.default_user
          unless default_user
            @logger.error("No default user #{default_user} for database #{name} when granting write access")
            return false
          end
          default_connection = postgresql_connect(
                                 db_connection.host,
                                 default_user[:user],
                                 default_user[:password],
                                 db_connection.port,
                                 name, :quick => true)
          unless default_connection
            @logger.error("Default user #{default_user} fail to connect to database #{name} when revoking write access")
            return false
          end
          schemas = get_conn_schemas(default_connection) || {}
          db_connection.transaction do |db_conn|
            public_schema_id ||= get_public_schema_id(db_conn)
            unless public_schema_id
              @logger.warn("Fail to get public schema id")
              return false
            end
            # revoke create privilege from database
            db_conn.query("revoke create on database #{name} from #{default_user[:user]}")

            # revoke write access from public shema
            revoke_schema_write_access(db_conn, public_schema_id, 'public', 'public')

            # revoke write privilege on all created schemas on the database
            schemas.each do |sc, sc_id|
              revoke_schema_write_access(db_conn, sc_id, sc, default_user[:user])
            end

            # revoke temp privilege on the database
            service.pgbindusers.all.each do |binduser|
              user = binduser.user
              sys_user = binduser.sys_user
              kill_alive_sessions(pgconn, :db => name, :users => [user])
              db_conn.query("REVOKE TEMP ON DATABASE #{name} from #{user}")
              db_conn.query("REVOKE TEMP ON DATABASE #{name} from #{sys_user}")
              do_revoke_query(db_conn, user, sys_user)
            end
          end
          service.quota_exceeded = true
          service.save
          return true
        rescue => e
          @logger.error("Fail to revoke write access for service #{service.name}: " + fmt_error(e))
          return false
        ensure
          default_connection.close if default_connection
        end

        # Return information of database
        def get_db_info(conn, db)
          return unless conn
          result = conn.query("select * from pg_database where datname='#{db}'")
          result[0]
        end

        # Drop database
        def exe_drop_database(conn, name)
          @logger ||= create_logger
          unless conn
            @logger.warn("No connection to drop database #{name}")
            return
          end
          @logger.info("Deleting database: #{name}")
          begin
            pid_field = pg_stat_activity_pid_field(pg_version(conn))
            conn.query("select pg_terminate_backend(#{pid_field}) from pg_stat_activity where datname = '#{name}'")
          rescue => e
            @logger.warn("Could not kill database session: #{e}")
          end
          drop_db(conn, name)
        end

        def drop_db(conn, db)
          return unless conn
          conn.query("drop database #{db}")
        end

        # Create database
        def exe_create_database(conn, name, max_db_conns)
          @logger ||= create_logger
          unless conn
            @logger.warn("No connection to create database #{name}")
            return
          end
          @logger.debug("Maximum connections: #{max_db_conns}")
          db_info = {}
          db_info["datconnlimit"] = max_db_conns if max_db_conns
          create_db(conn, name, db_info)
        end

        def create_db(conn, db, db_info)
          return unless conn
          if db_info["datconnlimit"]
            conn.query("create database #{db} with connection limit = #{db_info["datconnlimit"]}")
          else
            conn.query("create database #{db}")
          end
          conn.query("revoke all on database #{db} from public")
        end

        # Interrupt all activities on database
        def kill_alive_sessions(conn, opts={})
          return unless conn
          db = opts[:db]
          mode = (opts[:mode] || 'include')
          users = (opts[:users] || [])
          users << conn.user if mode != 'include'

          pid_field = pg_stat_activity_pid_field(pg_version(conn))
          q =  "select pg_terminate_backend(#{pid_field}) from pg_stat_activity"
          first_clause = true
          if db
            q += " where datname='#{db}'"
            first_clause = false
          end
          if users.count > 0
            q += first_clause ? " where" : " and"
            if users.count == 1
              q += " usename #{mode == 'include' ? '=' : '!='} '#{users[0]}'"
            else
              q += " usename #{mode == 'include' ? 'in' : 'not in'} (#{users.map {|u| "'#{u}'"}.join(',')})"
            end
          end
          conn.query(q)
        end

        # Block all binding users to connect the database
        def block_user_from_db(db_connection, service)
          name = service.name
          default_user = service.default_user
          service.pgbindusers.all.each do |binduser|
            if binduser.default_user == false
              db_connection.query("revoke #{default_user[:user]} from #{binduser.user}")
              db_connection.query("revoke connect on database #{name} from #{binduser.user}")
              db_connection.query("revoke connect on database #{name} from #{binduser.sys_user}")
            end
          end
        end

        # Permit all binding usrs to connect the database
        def unblock_user_from_db(db_connection, service)
          name = service.name
          default_user = service.default_user
          service.pgbindusers.all.each do |binduser|
            if binduser.default_user == false
              db_connection.query("GRANT CONNECT ON DATABASE #{name} to #{binduser.user}")
              db_connection.query("GRANT CONNECT ON DATABASE #{name} to #{binduser.sys_user}")
              db_connection.query("GRANT #{default_user[:user]} to #{binduser.user}")
            end
          end
        end

        # Block all users to connec the database
        def disable_db_conn(conn, db, service)
          return unless conn && service
          service.pgbindusers.each do |binduser|
            conn.query("revoke connect on database #{db} from #{binduser.user}")
            conn.query("revoke connect on database #{db} from #{binduser.sys_user}")
          end
        end

        # Enable all users to connect the dtabase
        def enable_db_conn(conn, db, service)
          return unless conn && service
          service.pgbindusers.each do |binduser|
            conn.query("grant connect on database #{db} to #{binduser.user}")
            conn.query("grant connect on database #{db} to #{binduser.sys_user}")
          end
        end

        # Check whether a connection is alive
        def connection_exception(conn)
          conn.query("select current_timestamp")
          return nil
        rescue => e
          @logger ||= create_logger
          @logger.warn("PostgreSQL connection #{(conn.inspect if conn)} lost: #{e}")
          return e
        end

        # Drop the database and re-create it for restoring/rolling back
        def reset_db(host, port, vcap_user, vcap_pass, database, service)
          pgconn = postgresql_connect(host, vcap_user, vcap_pass, port, database)
          name = service.name
          disable_db_conn(pgconn, name, service)
          kill_alive_sessions(pgconn, :db => name)
          db_info = get_db_info(pgconn, name)
          # we should considering re-set the privileges (such as create/temp ...) for parent role
          drop_db(pgconn, name)

          create_db(pgconn, name, db_info)
          enable_db_conn(pgconn, name, service)

          # should re-grant write privilege on database to parent role for restoring schemas
          # at the same time, for the database is recreated, the size should be under quota, it is safe to do this.
          # service.exceeded_quota should be false after this.
          dbconn = postgresql_connect(host, vcap_user, vcap_pass, port, name)
          unless grant_write_access_internal(dbconn, service)
            raise "Fail to grant write access when reseting the database #{name}"
          end
        ensure
          dbconn.close if dbconn
          pgconn.close if pgconn
        end

        # process command that need PGPASSFILE
        def pgpass_exe_cmd(name, host, port, user, passwd, cmd, &block)
          (pgpass_file = Tempfile.new('vcap_pgpass')).close
          FileUtils.chmod 0600, pgpass_file
          File.open(pgpass_file.path, 'w') { |f| f.puts "#{host}:#{port}:#{name}:#{user}:#{passwd}" }
          yield pgpass_file.path, cmd
        ensure
          # close and unlink
          pgpass_file.close!
        end

        # Use this method for backuping and snapshoting the database
        # name: name of database
        # host: ip/hostname of your database node
        # port: port of your database listening
        # user
        # passwd
        # dump_file: the file to store the dumped data
        # opts: optional arguments
        #   dump_bin
        def dump_database(name, host, port, user, passwd, dump_file, opts = {})
          raise "You must provide the following arguments: name, host, port, user, passwd, dump_file" unless name && host && port && user && passwd && dump_file

          dump_bin = opts[:dump_bin] || 'pg_dump'
          dump_cmd = "#{dump_bin} -Fc --host=#{host} --port=#{port} --username=#{user} --file=#{dump_file} #{name}"

          pgpass_exe_cmd(name, host, port, user, passwd, dump_cmd) do |pgpass_file, pgpass_cmd|
            o, e, s = exe_cmd(pgpass_cmd, {'PGPASSFILE' => pgpass_file})
            return s.exitstatus == 0
          end
        end

        # Use this method to filter the un-supported archive elements in HACK style
        def archive_list(name, host, port, user, passwd, dump_file, opts = {})
          raise "You must provide the following arguments: name, host, port, user, passwd, dump_file" unless name && host && port && user && passwd && dump_file

          restore_bin = opts[:restore_bin] || 'pg_restore'
          # HACK: exclude the commands that result privllege issue during import
          exclude_cmd_patterns = [
            "COMMENT - EXTENSION plpgsql",
            "PROCEDURAL LANGUAGE - plpgsql"
          ]
          exclude_cmd = exclude_cmd_patterns.map{|pattern| "grep -v '#{pattern}'"}.join(" | ")
          cmd = "#{restore_bin} -l #{dump_file} | #{exclude_cmd} > #{dump_file}.archive_list"

          pgpass_exe_cmd(name, host, port, user, passwd, cmd) do |pgpass_file, pgpass_cmd|
            o, e, s = exe_cmd(pgpass_cmd, {'PGPASSFILE' => pgpass_file})
            return s.exitstatus == 0
          end
        end

        # Use this method for restoring and importing the database
        # name: name of database
        # host: ip/hostname of your database node
        # port: port of your database listening
        # user
        # passwd
        # dump_file: the file which stores the dumped data
        # opts: optional arguments
        #   restore_bin
        def restore_database(name, host, port, user, passwd, dump_file, opts = {})
          raise "You must provide the following arguments: name, host, port, user, passwd, dump_file" unless name && host && port && user && passwd && dump_file

          return false unless archive_list(name, host, port, user, passwd, dump_file, opts)
          restore_bin = opts[:restore_bin] || 'pg_restore'
          restore_cmd = "#{restore_bin} -h #{host} -p #{port} -U #{user} -L #{dump_file}.archive_list -d #{name} #{dump_file} "

          pgpass_exe_cmd(name, host, port, user, passwd, restore_cmd) do |pgpass_file, pgpass_cmd|
            o, e, s = exe_cmd(pgpass_cmd, {'PGPASSFILE' => pgpass_file})
            return s.exitstatus == 0
          end
        ensure
          FileUtils.rm_rf("#{dump_file}.archive_list")
        end

        def is_default_bind_user(user_name)
          if respond_to?(:pgBindUser)
            user = pgBindUser.get(user_name)
            !user.nil? && user.default_user
          else
            return false
          end
        end

        def kill_long_queries_internal(connection, super_user, max_long_query)
          @logger ||= create_logger
          long_queries_killed = 0
          unless connection && super_user && max_long_query
            @logger.warn("Invalid parameters to kill long queries: #{connection}, #{super_user}, #{max_long_query}")
            return long_queries_killed
          end

          begin
            version = pg_version(connection)
            pid_field = pg_stat_activity_pid_field(version)
            query_field = pg_stat_activity_query_field(version)
            process_list = connection.query(
              "select * from (select #{pid_field} as t_pid, datname, query_start, usename,
              (extract(epoch from current_timestamp) - extract(epoch from query_start)) as run_time,
              #{query_field} as t_query from pg_stat_activity where query_start is not NULL and usename != '#{super_user}'
              and #{query_field} !='<IDLE>' and #{query_field} != '<IDLE> in transaction')
              as inner_table  where run_time > #{max_long_query}")
            process_list.each do |proc|
              unless is_default_bind_user(proc["usename"])
                # Cancel the exact query when exceeding the query time limitation
                res = connection.query("select pg_cancel_backend(#{proc['t_pid']}) from pg_stat_activity
                                        where #{pid_field} = #{proc['t_pid']} and query_start = (timestamp '#{proc['query_start']}')")
                res.each do |cancel_query|
                  if cancel_query['pg_cancel_backend'] == 't'
                    @logger.info("Killed long query: user:#{proc['usename']} db:#{proc['datname']} time:#{proc['run_time']} info:#{proc['t_query']}")
                    long_queries_killed += 1
                  end
                end
              end
            end
          rescue => e
            @logger.warn("PostgreSQL error: #{e}")
          end
          long_queries_killed
        end

        def kill_long_transaction_internal(connection, super_user, max_long_tx)
          @logger ||= create_logger
          long_tx_killed = 0
          unless connection && super_user && max_long_tx
            @logger.warn("Invalid parameters to kill long tx: #{connection}, #{super_user}, #{max_long_tx}")
            return long_tx_killed
          end
          begin
            version = pg_version(connection)
            pid_field = pg_stat_activity_pid_field(version)
            query_field = pg_stat_activity_query_field(version)

            process_list = connection.query(
              "select * from (select #{pid_field} as t_pid, datname, xact_start, usename,
              (extract(epoch from current_timestamp) - extract(epoch from xact_start)) as run_time,
              #{query_field} as t_query from pg_stat_activity where xact_start is not NULL and usename != '#{super_user}')
              as inner_table where run_time > #{max_long_tx}")
            process_list.each do |proc|
              unless is_default_bind_user(proc["usename"])
                # Terminate the connection when exceeding the transaction time limitation
                res = connection.query("select #{query_field} as t_query, pg_terminate_backend(#{proc['t_pid']}) from pg_stat_activity
                                        where #{pid_field} = #{proc['t_pid']} and xact_start = (timestamp '#{proc['xact_start']}')")
                res.each do |term_query|
                  if term_query['pg_terminate_backend'] == "t"
                    @logger.info("Killed long transaction: user:#{proc['usename']} db:#{proc['datname']} active_time:#{proc['run_time']} info:#{term_query['t_query']}")
                    long_tx_killed += 1
                  end
                end
              end
            end
          rescue => e
            @logger.warn("PostgreSQL error: #{e}")
          end
          long_tx_killed
        end

        def get_db_stat_by_connection(connection, max_db_size, sys_dbs=[])
          @logger ||= create_logger
          result = []
          return result unless connection
          db_stats = connection.query('select datid, datname, version() as version from pg_stat_database')
          db_stats.each do |d|
            name = d["datname"]
            oid = d["datid"]
            version = d["version"]
            next if sys_dbs.include?(name)
            db = {}
            # db name
            db[:name] = name
            # db verison
            db[:version] = version
            # db max size
            db[:max_size] = max_db_size
            # db actual size
            sizes = connection.query("select pg_database_size('#{name}')")
            db[:size] = sizes[0]['pg_database_size'].to_i
            # db active connections
            a_s_ps = connection.query("select pg_stat_get_db_numbackends(#{oid})")
            db[:active_server_processes] = a_s_ps[0]['pg_stat_get_db_numbackends'].to_i
            result << db
          end
          result
        rescue => e
          @logger.warn("Error during generate varz/db_stat: #{e}")
          []
        end

        def get_db_list_by_connection(connection)
          @logger ||= create_logger
          db_list = []
          return db_list unless connection
          connection.query('select datname,datacl from pg_database').each{ |message|
            datname = message['datname']
            datacl = message['datacl']
            if not datacl==nil
              users = datacl[1,datacl.length-1].split(',')
              for user in users do
                db_list.push([datname, user.split('=')[0]]) unless user.split('=')[0].empty?
              end
            end
          }
          db_list
        rescue => e
          @logger.error("Fail to get db list using connection #{connection} for #{fmt_error(e)}")
          []
        end

      end
    end
  end
end
