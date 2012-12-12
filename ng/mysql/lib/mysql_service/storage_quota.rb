# Copyright (c) 2009-2011 VMware, Inc.
require "mysql2"
require "mysql_service/util"

module VCAP; module Services; module Mysql; end; end; end

class VCAP::Services::Mysql::Node

  DATA_LENGTH_FIELD = 6

  def system_and_extra_size(connection, dbs_size)
    extra_sizes = connection.query('SHOW VARIABLES LIKE "innodb_%"')
    result = {}
    extra_sizes.each do |i|
      name, size = i["Variable_name"], i["Value"]
      result[name] = size.to_i
    end
    total_sizes = [dbs_size["mysql"], dbs_size["information_schema"],
                   result["innodb_autoextend_increment"] * 1024 * 1024,
                   result["innodb_log_file_size"] * result["innodb_log_files_in_group"]]
    total_sizes.inject(:+)
  end

  def dbs_size(connection, dbs=[])
    dbs = [] if dbs.nil?
    if dbs.length == 0
      result = connection.query('show databases')
      result.each {|db| dbs << db["Database"]}
    end
    sizes = connection.query(
      'SELECT table_schema "name",
       sum( IF(engine = "MyISAM",
       data_length + index_length - data_free,
       data_length + index_length) ) "size"
       FROM information_schema.TABLES
       GROUP BY table_schema')
    result ={}
    sizes.each do |i|
      name, size = i["name"], i["size"]
      result[name] = size.to_i
    end
    # assume 0 size for db which has no tables
    dbs.each {|db| result[db] = 0 unless result.has_key? db}
    extra_size = extra_size_per_db(connection, result)
    result.each do |db, _|
      result[db] += extra_size
    end
    result
  end

  def kill_user_sessions(target_user, target_db)
    fetch_pool(target_db).with_connection do |connection|
      process_list = connection.query("show processlist")
      process_list.each do |proc|
        thread_id, user, db = proc["Id"], proc["User"], proc["db"]
        if (user == target_user) and (db == target_db) then
          connection.query('KILL CONNECTION ' + thread_id)
        end
      end
    end
  end

  def access_disabled?(db)
    fetch_pool(db).with_connection do |connection|
      rights = connection.query("SELECT insert_priv, create_priv, update_priv
                                  FROM db WHERE Db=" +  "'#{db}'")
      rights.each do |right|
        return false if right.values.include? 'Y'
      end
    end
    true
  end

  def grant_write_access(db, service)
    @logger.warn("DB permissions inconsistent....") unless access_disabled?(db)
    fetch_pool(service.name).with_connection do |connection|
      connection.query("UPDATE db SET insert_priv='Y', create_priv='Y',
                         update_priv='Y' WHERE Db=" +  "'#{db}'")
      connection.query("FLUSH PRIVILEGES")
      # kill existing session so that privilege take effect
      kill_database_session(connection, db)
    end
    service.quota_exceeded = false
    service.save
  end

  def revoke_write_access(db, service)
    @logger.warn("DB permissions inconsistent....") if access_disabled?(db)
    fetch_pool(service.name).with_connection do |connection|
      connection.query("UPDATE db SET insert_priv='N', create_priv='N',
                         update_priv='N' WHERE Db=" +  "'#{db}'")
      connection.query("FLUSH PRIVILEGES")
      kill_database_session(connection, db)
    end
    service.quota_exceeded = true
    service.save
  end

  def fmt_db_listing(user, db, size)
    "<user: '#{user}' name: '#{db}' size: #{size}>"
  end

  def enforce_storage_quota
    acquired = @enforce_quota_lock.try_lock
    return unless acquired
    sizes = {}
    each_connection do |connection|
      connection.query('use mysql')
      sizes.merge!(dbs_size(connection))
    end
    mysqlProvisionedService.all.each do |service|
      begin
        db, user, quota_exceeded = service.name, service.user, service.quota_exceeded
        size = sizes[db]
        # ignore the orphan instance
        next if size.nil?

        if (size >= @max_db_size) and not quota_exceeded then
          revoke_write_access(db, service)
          @logger.info("Storage quota exceeded :" + fmt_db_listing(user, db, size) +
                       " -- access revoked")
        elsif (size < @max_db_size) and quota_exceeded then
          grant_write_access(db, service)
          @logger.info("Below storage quota:" + fmt_db_listing(user, db, size) +
                       " -- access restored")
        end
      rescue => e1
        @logger.warn("Fail to enfroce storage quota on #{service.name}: #{e1}" + e1.backtrace.join("|") )
      end
    end
  rescue Mysql2::Error => e
    @logger.warn("MySQL exception: [#{e.errno}] #{e.error} " +
                   e.backtrace.join("|"))
  ensure
    @enforce_quota_lock.unlock if acquired
  end

  # when binding a new application, should check whether to revoke the new user's write access for enforce_storage_quota may have set quota_exceeded already.
  def enforce_instance_storage_quota(service)
    begin
      db, user, quota_exceeded = service.name, service.user, service.quota_exceeded
      sizes = {}
      fetch_pool(service.name).with_connection do |connection|
        connection.query('use mysql')
        sizes.merge!(dbs_size(connection, [db]))
      end
      size = sizes[db]
      return if size.nil?
      if size >= @max_db_size then
        revoke_write_access(db, service)
        @logger.info("Instance storage quota exceeded :" + fmt_db_listing(user, db, size) +
                     " -- access revoked")
      elsif (size < @max_db_size) and quota_exceeded then
        grant_write_access(db, service)
        @logger.info("Below instance storage quota:" + fmt_db_listing(user, db, size) +
                     " -- access restored")
      end
    rescue => e
      @logger.warn("Fail to enforce the storage quota on #{service.name}: #{e}" + e.backtrace.join("|") )
    end
  end

end
