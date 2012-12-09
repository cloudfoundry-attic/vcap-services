# Copyright (c) 2009-2011 VMware, Inc.
require "pg"

module VCAP; module Services; module Postgresql; end; end; end

class VCAP::Services::Postgresql::Node

  def fmt_db_listing(name, size)
    "<name: '#{name}' size: #{size}>"
  end

  def revoke_write_access(service)
    name = service.name
    db_connection = management_connection(service, true, :quick => true)
    if db_connection.nil?
      @logger.warn("Unable to revoke write access to #{name}: fail to connect to #{name}")
      return false
    end
    global_conn = global_connection(service)
    @public_schema_id ||= get_public_schema_id(global_conn)
    unless @public_schema_id
      @logger.warn("Unable to revoke write access to #{name}: fail to retrieve info of public schema.")
      return false
    end
    return revoke_write_access_internal(global_conn, db_connection, service, @public_schema_id)
  ensure
    db_connection.close if db_connection
  end

  def grant_write_access(service)
    name = service.name
    db_connection = management_connection(service, true, :quick => true)
    if db_connection.nil?
      @logger.warn("Unable to grant write access to #{name}: fail to connect to #{name}")
      return false
    end
    global_conn = global_connection(service)
    @public_schema_id ||= get_public_schema_id(global_conn)
    unless @public_schema_id
      @logger.warn("Unable to revoke write access to #{name}: fail to retrieve info of public schema.")
      return false
    end
    return grant_write_access_internal(db_connection, service, @public_schema_id)
  ensure
    db_connection.close if db_connection
  end

  def enforce_storage_quota
    acquired = @enforce_quota_lock.try_lock
    return unless acquired
    sizes = dbs_size()
    pgProvisionedService.all.each do |service|
      enforce_instance_storage_quota(service, sizes[service.name])
    end
  rescue => e
    @logger.warn("PostgreSQL Node exception: " + fmt_error(e))
  ensure
    @enforce_quota_lock.unlock if acquired
  end

  def enforce_instance_storage_quota(service, database_size=nil)
    begin
      name, quota_exceeded = service.name, service.quota_exceeded
      size = database_size || db_size(service)
      if size.nil?
        @logger.warn("Could not get the size of #{name} in Postgresql")
        return
      end

      if (size >= @max_db_size) and not quota_exceeded then
          if revoke_write_access(service)
            @logger.info("Storage quota exceeded :" + fmt_db_listing(name, size) +
                    " -- access revoked")
          else
            @logger.warn("Storage quota exceeded:" + fmt_db_listing(name, size) +
                    " -- fail to reovke access")
          end
      elsif (size < @max_db_size) and quota_exceeded then
          if grant_write_access(service)
            @logger.info("Below storage quota:" + fmt_db_listing(name, size) +
                    " -- access restored")
          else
            @logger.warn("Below storage quota:" + fmt_db_listing(name, size) +
                    " -- but fail to restore access")
          end
      end
    rescue => e
      @logger.warn("Fail to enforce storage quota for service #{service.name}: " + fmt_error(e))
    end
  end

end
