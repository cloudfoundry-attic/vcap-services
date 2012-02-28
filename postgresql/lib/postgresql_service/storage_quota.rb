# Copyright (c) 2009-2011 VMware, Inc.
require "pg"

module VCAP; module Services; module Postgresql; end; end; end

class VCAP::Services::Postgresql::Node

  def db_size(db)
    sz = @connection.query("select pg_database_size('#{db}') size")
    sum = 0
    sz.each do |x|
      sum += x['size'].to_i
    end
    sum
  end

  def kill_user_sessions(target_user, target_db)
    @connection.query("select pg_terminate_backend(procpid) from pg_stat_activity where usename = '#{target_user}' and datname = '#{target_db}'")
  end

  #Grant access without checking
  def grant_write_access(db, service)
    name = service.name
    db_connection = postgresql_connect(@postgresql_config["host"], @postgresql_config["user"], @postgresql_config["pass"], @postgresql_config["port"], name, true)
    if db_connection.nil?
      @logger.warn("Unable to grant write access to #{name}")
      return
    end
    service.bindusers.all.each do |binduser|
      user = binduser.user
      sys_user = binduser.sys_user
      sys_password = binduser.sys_password
      db_connection_sys_user = postgresql_connect(@postgresql_config["host"], sys_user, sys_password, @postgresql_config["port"], name, true)
      if db_connection_sys_user.nil?
        @logger.warn("Unable to grant write access to #{name} for #{sys_user}")
      else
        db_connection_sys_user.query("vacuum full")
        db_connection_sys_user.close
        do_grant_query(db_connection,user,sys_user)
      end
    end
    db_connection.query("grant create on schema public to public")
    if get_postgres_version(db_connection) == '9'
      db_connection.query("grant all on all tables in schema public to public")
      db_connection.query("grant all on all sequences in schema public to public")
      db_connection.query("grant all on all functions in schema public to public")
    else
      querys = db_connection.query("select 'grant all on '||tablename||' to public;' as query_to_do from pg_tables where schemaname = 'public'")
      querys.each do |query_to_do|
         db_connection.query(query_to_do['query_to_do'].to_s)
      end
      querys = db_connection.query("select 'grant all on sequence '||relname||' to public;' as query_to_do from pg_class where relkind = 'S'")
      querys.each do |query_to_do|
         db_connection.query(query_to_do['query_to_do'].to_s)
      end
    end
    db_connection.close
    service.quota_exceeded = false
    service.save
  rescue => e
      @logger.warn("PostgreSQL Node exception: #{e} " +
                    e.backtrace.join("|"))
  end

  def do_grant_query(db_connection,user,sys_user)
    db_connection.query("update pg_class set relowner = (select oid from pg_roles where rolname = '#{user}') where relowner = (select oid from pg_roles where rolname = '#{sys_user}')")
  end

  def revoke_write_access(db, service)
    name = service.name
    db_connection = postgresql_connect(@postgresql_config["host"], @postgresql_config["user"], @postgresql_config["pass"], @postgresql_config["port"], name, true)
    if db_connection.nil?
      @logger.warn("Unable to revoke write access on #{name}")
      return
    end
    db_connection.query("revoke create on schema public from public CASCADE")
    if get_postgres_version(db_connection) == '9'
      db_connection.query("REVOKE ALL ON ALL TABLES IN SCHEMA PUBLIC from public CASCADE")
      db_connection.query("REVOKE ALL ON ALL SEQUENCES IN SCHEMA PUBLIC from public CASCADE")
      db_connection.query("REVOKE ALL ON ALL FUNCTIONS IN SCHEMA PUBLIC from public CASCADE")
      db_connection.query("grant select,delete,truncate,references,trigger on all tables in schema public to public")
      db_connection.query("grant usage,select on all sequences in schema public to public")
    else
      querys = db_connection.query("select 'REVOKE ALL ON '||tablename||' from public CASCADE;' as query_to_do from pg_tables where schemaname = 'public'")
      querys.each do |query_to_do|
         db_connection.query(query_to_do['query_to_do'].to_s)
      end
      querys = db_connection.query("select 'REVOKE ALL ON SEQUENCE '||relname||' from public CASCADE;' as query_to_do from pg_class where relkind = 'S'")
      querys.each do |query_to_do|
         db_connection.query(query_to_do['query_to_do'].to_s)
      end
      querys = db_connection.query("select 'grant select,delete,truncate,references,trigger on '||tablename||' to public;' as query_to_do from pg_tables where schemaname = 'public'")
      querys.each do |query_to_do|
         db_connection.query(query_to_do['query_to_do'].to_s)
      end
      querys = db_connection.query("select 'grant usage,select on SEQUENCE '||relname||' to public;' as query_to_do from pg_class where relkind = 'S'")
      querys.each do |query_to_do|
         db_connection.query(query_to_do['query_to_do'].to_s)
      end
    end
    service.bindusers.all.each do |binduser|
      user = binduser.user
      sys_user = binduser.sys_user
      kill_user_sessions(user, name)
      do_revoke_query(db_connection, user, sys_user)
    end
    db_connection.close
    service.quota_exceeded = true
    service.save
  rescue => e
      @logger.warn("PostgreSQL Node exception: #{e} " +
                    e.backtrace.join("|"))
  end

  def do_revoke_query(db_connection, user, sys_user)
    db_connection.query("revoke create on schema public from #{user} CASCADE")
    if get_postgres_version(db_connection) == '9'
      db_connection.query("REVOKE ALL ON ALL TABLES IN SCHEMA PUBLIC from #{user} CASCADE")
      db_connection.query("REVOKE ALL ON ALL SEQUENCES IN SCHEMA PUBLIC from #{user} CASCADE")
      db_connection.query("REVOKE ALL ON ALL FUNCTIONS IN SCHEMA PUBLIC from #{user} CASCADE")
    else
      querys = db_connection.query("select 'REVOKE ALL ON '||tablename||' from #{user} CASCADE;' as query_to_do from pg_tables where schemaname = 'public'")
      querys.each do |query_to_do|
         db_connection.query(query_to_do['query_to_do'].to_s)
      end
      querys = db_connection.query("select 'REVOKE ALL ON SEQUENCE '||relname||' from #{user} CASCADE;' as query_to_do from pg_class where relkind = 'S'")
      querys.each do |query_to_do|
         db_connection.query(query_to_do['query_to_do'].to_s)
      end
    end

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

  def fmt_db_listing(db, size)
    "<name: '#{db}' size: #{size}>"
  end

  def enforce_storage_quota
    Provisionedservice.all.each do |service|
      begin
        db, quota_exceeded = service.name, service.quota_exceeded
        size = db_size(db)
        if (size >= @max_db_size) and not quota_exceeded then
          revoke_write_access(db, service)
          @logger.info("Storage quota exceeded :" + fmt_db_listing(db, size) +
                    " -- access revoked")
        elsif (size < @max_db_size) and quota_exceeded then
          grant_write_access(db, service)
          @logger.info("Below storage quota:" + fmt_db_listing(db, size) +
                    " -- access restored")
        end
      rescue => e
        @logger.warn("PostgreSQL Node exception: #{e} " + e.backtrace.join("|"))
      end
    end
  end

end
