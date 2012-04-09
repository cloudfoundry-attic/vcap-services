# Copyright (c) 2009-2011 VMware, Inc.
module VCAP
  module Services
    module Postgresql

      # Give various helper functions
      module Util
        VALID_CREDENTIAL_CHARACTERS = ("A".."Z").to_a + ("a".."z").to_a + ("0".."9").to_a

        def parse_property(hash, key, type, options = {})
          obj = hash[key]
          if obj.nil?
            raise "Missing required option: #{key}" unless options[:optional]
            nil
          elsif type == Range
            raise "Invalid Range object: #{obj}" unless obj.kind_of?(Hash)
            first, last = obj["first"], obj["last"]
            raise "Invalid Range object: #{obj}" unless first.kind_of?(Integer) and last.kind_of?(Integer)
            Range.new(first, last)
          else
            raise "Invalid #{type} object: #{obj}" unless obj.kind_of?(type)
            obj
          end
        end

        def fmt_error(e)
          "#{e}: [#{e.backtrace.join(" | ")}]"
        end

        def create_logger(logdev, rotation, level)
          if String === logdev
            dir = File.dirname(logdev)
            FileUtils.mkdir_p(dir) unless File.directory?(dir)
          end
          logger = Logger.new(logdev, rotation)
          logger.level = case level
            when "DEBUG" then Logger::DEBUG
            when "INFO" then Logger::INFO
            when "WARN" then Logger::WARN
            when "ERROR" then Logger::ERROR
            when "FATAL" then Logger::FATAL
            else Logger::UNKNOWN
          end
          logger
        end

        def generate_credential(length=12)
          Array.new(length) { VALID_CREDENTIAL_CHARACTERS[rand(VALID_CREDENTIAL_CHARACTERS.length)] }.join
        end

        # Return the version of postgresql
        def pg_version(conn)
          return '-1' unless conn
          version = conn.query("select version()")
          reg = /([0-9.]{5})/
          return version[0]['version'].scan(reg)[0][0][0]
        end

        def reset_owner(conn, name, owner)
          return unless conn
          conn.query("alter database #{name} owner to #{owner}")
        end

        def grant_user_priv(conn, version)
          return unless conn
          conn.query("grant create on schema public to public")
          if version == '9'
            conn.query("grant all on all tables in schema public to public")
            conn.query("grant all on all sequences in schema public to public")
            conn.query("grant all on all functions in schema public to public")
          else
            querys = conn.query("select 'grant all on '||tablename||' to public;' as query_to_do from pg_tables where schemaname = 'public'")
            querys.each do |query_to_do|
              conn.query(query_to_do['query_to_do'].to_s)
            end
            querys = conn.query("select 'grant all on sequence '||relname||' to public;' as query_to_do from pg_class where relkind = 'S'")
            querys.each do |query_to_do|
              conn.query(query_to_do['query_to_do'].to_s)
            end
          end
        end

        def get_db_info(conn, db)
          return unless conn
          result = conn.query("select * from pg_database where datname='#{db}'")
          result[0]
        end

        def drop_db(conn, db)
            return unless conn
          conn.query("drop database #{db}")
        end

        def create_db(conn, db, db_info)
          return unless conn
          if db_info["datconnlimit"].nil?
            conn.query("create database #{db} with connection limit = #{db_info["datconnlimit"]}")
          else
            conn.query("create database #{db}")
          end
          conn.query("revoke all on database #{db} from public")
        end

        def kill_alive_sessions(conn, db)
          return unless conn
          conn.query("select pg_terminate_backend(procpid) from pg_stat_activity where datname='#{db}'")
        end

        def disable_db_conn(conn, db, service)
          return unless conn && service
          service.bindusers.each do |binduser|
            conn.query("revoke connect on database #{db} from #{binduser.user}")
            conn.query("revoke connect on database #{db} from #{binduser.sys_user}")
          end
        end

        def enable_db_conn(conn, db, service)
          return unless conn && service
          service.bindusers.each do |binduser|
            conn.query("grant connect on database #{db} to #{binduser.user}")
            conn.query("grant connect on database #{db} to #{binduser.sys_user}")
          end
        end


        def execute_shell_cmd(cmd, env={}, stdin=nil, logger=nil)
          o, e, s = Open3.capture3(env, cmd, :stdin_data => stdin)
          [o, e, s]
        end

        def reset_db(host, port, vcap_user, vcap_pass, name, service)
          pgconn = PGconn.new(host, port, nil, nil, "postgres", vcap_user, vcap_pass)
          disable_db_conn(pgconn, name, service)

          kill_alive_sessions(pgconn, name)
          db_info = get_db_info(pgconn, name)
          db_version = pg_version(pgconn)

          drop_db(pgconn, name)

          create_db(pgconn, name, db_info)

          dbconn = PGconn.new(host, port, nil, nil, name, vcap_user, vcap_pass)
          grant_user_priv(dbconn, db_version)
          dbconn.close

          enable_db_conn(pgconn, name, service)

          pgconn.close
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
        #   logger
        def dump_database(name, host, port, user, passwd, dump_file, opts = {} )
          raise "You must provide the following arguments: name, host, port, user, passwd, dump_file" unless name && host && port && user && passwd && dump_file

          dump_bin = opts[:dump_bin] || 'pg_dump'
          dump_cmd = "#{dump_bin} -Fc --host=#{host} --port=#{port} --username=#{user} --file=#{dump_file} #{name}"

          # running the command
          on_err = Proc.new do |cmd, code, msg|
            opts[:logger].error("CMD '#{cmd}' exit with code: #{code} & Message: #{msg}") if opts[:logger]  && opts[:logger].respond_to?(:error)
          end

          result = CMDHandle.execute(dump_cmd, nil, on_err )
          raise "Failed to dump database of #{name}" unless result
          result
        end


        def archive_list(dump_file, opts = {})
          restore_bin = opts[:restore_bin] || 'pg_restore'
          cmd = "#{restore_bin} -l #{dump_file} | grep -v 'PROCEDURAL LANGUAGE - plpgsql' > #{dump_file}.archive_list"
          o, e, s = execute_shell_cmd(cmd)
          return s.exitstatus == 0
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
        #   logger
        def restore_database(name, host, port, user, passwd, dump_file, opts = {} )
          raise "You must provide the following arguments: name, host, port, user, passwd, dump_file" unless name && host && port && user && passwd && dump_file

          archive_list(dump_file, opts )

          restore_bin = opts[:restore_bin] || 'pg_restore'
          restore_cmd = "#{restore_bin} -h #{host} -p #{port} -U #{user} -L #{dump_file}.archive_list -d #{name} #{dump_file} "

          # running the command
          o, e, s = execute_shell_cmd(restore_cmd)
          return s.exitstatus == 0
        ensure
          FileUtils.rm_rf("#{dump_file}.archive_list")
        end

      end
    end
  end
end
