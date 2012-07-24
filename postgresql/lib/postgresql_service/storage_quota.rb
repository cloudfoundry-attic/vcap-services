# Copyright (c) 2009-2011 VMware, Inc.
require "pg"

module VCAP; module Services; module Postgresql; end; end; end

class VCAP::Services::Postgresql::Node

  def dbs_size(dbs=[])
    dbs = [] if dbs.nil?

    result = {}
    res = @connection.query('select datname, sum(pg_database_size(datname)) as sum_size from pg_database group by datname')
    res.each do |x|
      name, size = x["datname"], x["sum_size"]
      result[name] = size.to_i
    end

    if dbs.length > 0
      dbs.each {|db| result[db] = 0 unless result.has_key? db}
    end
    result
  end

  def db_size(db)
    sz = @connection.query("select pg_database_size('#{db}') size")
    sum = 0
    sz.each do |x|
      sum += x['size'].to_i
    end
    sum
  end

  def fmt_db_listing(db, size)
    "<name: '#{db}' size: #{size}>"
  end

  def revoke_write_access(name, service)
    db_connection = postgresql_connect(@postgresql_config["host"], @postgresql_config["user"], @postgresql_config["pass"], @postgresql_config["port"], name, true)
    if db_connection.nil?
      @logger.warn("Unable to revoke write access to #{name}: fail to connect to #{name}")
      return false
    end
    @public_schema_id ||= get_public_schema_id(@connection)
    unless @public_schema_id
      @logger.warn("Unable to revoke write access to #{name}: fail to retrieve info of public schema.")
      return false
    end
    return revoke_write_access_internal(@connection, db_connection, service, @public_schema_id)
  ensure
    db_connection.close if db_connection
  end

  def grant_write_access(name, service)
    db_connection = postgresql_connect(@postgresql_config["host"], @postgresql_config["user"], @postgresql_config["pass"], @postgresql_config["port"], name, true)
    if db_connection.nil?
      @logger.warn("Unable to grant write access to #{name}: fail to connect to #{name}")
      return false
    end
    @public_schema_id ||= get_public_schema_id(@connection)
    unless @public_schema_id
      @logger.warn("Unable to revoke write access to #{name}: fail to retrieve info of public schema.")
      return false
    end
    return grant_write_access_internal(db_connection, service, @public_schema_id)
  ensure
    db_connection.close if db_connection
  end

  def enforce_storage_quota
    sizes = dbs_size()

    Provisionedservice.all.each do |service|
      enforce_instance_storage_quota(service, sizes[service.name])
    end
  rescue => e
    @logger.warn("PostgreSQL Node exception: " + fmt_error(e))
  end

  def enforce_instance_storage_quota(service, database_size=nil)
    begin
      name, quota_exceeded = service.name, service.quota_exceeded
      size = database_size || db_size(name)
      if size.nil?
        @logger.warn("Could not get the size of #{name} in Postgresql")
        return
      end

      if (size >= @max_db_size) and not quota_exceeded then
          if revoke_write_access(name, service)
            @logger.info("Storage quota exceeded :" + fmt_db_listing(name, size) +
                    " -- access revoked")
          else
            @logger.warn("Storage quota exceeded:" + fmt_db_listing(name, size) +
                    " -- fail to reovke access")
          end
      elsif (size < @max_db_size) and quota_exceeded then
          if grant_write_access(name, service)
            @logger.info("Below storage quota:" + fmt_db_listing(name, size) +
                    " -- access restored")
          else
            @logger.warn("Below storage quota:" + fmt_db_listing(name, size) +
                    " -- but fail to restore access")
          end
      end
    rescue => e
      @logger.warn("PostgreSQL Node exception: " + fmt_error(e))
    end
  end

end
