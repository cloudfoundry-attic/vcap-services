# Copyright (c) 2009-2011 VMware, Inc.
require "erb"
require "fileutils"
require "logger"
require "pp"

require "datamapper"
require "uuidtools"
require "mysql"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..', 'base', 'lib')
require 'base/node'

module VCAP
  module Services
    module Mysql
      class Node < VCAP::Services::Base::Node
      end
    end
  end
end

require "mysql_service/common"
require "mysql_service/util"
require "mysql_service/storage_quota"

class VCAP::Services::Mysql::Node

  KEEP_ALIVE_INTERVAL = 15
  LONG_QUERY_INTERVAL = 1
  STORAGE_QUOTA_INTERVAL = 1

  include VCAP::Services::Mysql::Util
  include VCAP::Services::Mysql::Common

  class ProvisionedService
    include DataMapper::Resource
    property :name,       String,   :key => true
    property :user,       String,   :required => true
    property :password,   String,   :required => true
    property :plan,       Enum[:free], :required => true
    property :quota_exceeded,  Boolean, :default => false
  end

  def initialize(options)
    super(options)

    @mysql_config = options[:mysql]

    @max_db_size = options[:max_db_size] * 1024 * 1024
    @max_long_query = options[:max_long_query]
    @max_long_tx = options[:max_long_tx]

    @connection = mysql_connect

    EM.add_periodic_timer(KEEP_ALIVE_INTERVAL) {mysql_keep_alive}
    EM.add_periodic_timer(LONG_QUERY_INTERVAL) {kill_long_queries}
    EM.add_periodic_timer(@max_long_tx/2) {kill_long_transaction}
    EM.add_periodic_timer(STORAGE_QUOTA_INTERVAL) {enforce_storage_quota}

    @base_dir = options[:base_dir]
    FileUtils.mkdir_p(@base_dir) if @base_dir

    DataMapper.setup(:default, options[:local_db])
    DataMapper::auto_upgrade!

    check_db_consistency()

    @available_storage = options[:available_storage] * 1024 * 1024
    ProvisionedService.all.each do |provisioned_service|
      @available_storage -= storage_for_service(provisioned_service)
    end

  end

  def announcement
    a = {
      :available_storage => @available_storage
    }
    a
  end

  def check_db_consistency()
    db_list = []
    @connection.query('select db, user from db').each{|db, user| db_list.push([db, user])}
    ProvisionedService.all.each do |service|
      db, user = service.name, service.user
      if not db_list.include?([db, user]) then
        @logger.info("Node database inconsistent!!! db:user <#{db}:#{user}> not in mysql.")
        next
      end
    end
  end

  def storage_for_service(provisioned_service)
    case provisioned_service.plan
    when :free then @max_db_size
    else
      raise "Invalid plan: #{provisioned_service.plan}"
    end
  end

  def mysql_connect
    host, user, password, port, socket =  %w{host user pass port socket}.map { |opt| @mysql_config[opt] }

    5.times do
      begin
        return Mysql.real_connect(host, user, password, 'mysql', port.to_i, socket)
      rescue Mysql::Error => e
        @logger.info("MySQL connection attempt failed: [#{e.errno}] #{e.error}")
        sleep(5)
      end
    end

    @logger.fatal("MySQL connection unrecoverable")
    shutdown
    exit
  end

  #keep connection alive, and check db liveness
  def mysql_keep_alive
    @connection.ping()
  rescue Mysql::Error => e
    @logger.info("MySQL connection lost: [#{e.errno}] #{e.error}")
    @connection = mysql_connect
  end

  def kill_long_queries
    process_list = @connection.list_processes
    process_list.each do |proc|
      thread_id, user, _, db, command, time, _, info = proc
      if (time.to_i >= @max_long_query) and (command == 'Query') and (user != 'root') then
        @connection.query("KILL QUERY " + thread_id)
        @logger.info("Killed long query: user:#{user} db:#{db} time:#{time} info:#{info}")
      end
    end
  rescue Mysql::Error => e
    @logger.info("MySQL error: [#{e.errno}] #{e.error}")
  end

  def kill_long_transaction
    # FIXME need a better transaction query solution other than parse status text
    result = @connection.query("SHOW INNODB STATUS")
    innodb_status = nil
    result.each do |i|
      innodb_status = i[-1]
    end
    lines = innodb_status.split(/\n/).map{|line| line.strip}
    i = 0
    while i<= lines.size
      if lines[i] =~ /---TRANSACTION.*ACTIVE (\d*) sec/ && $1.to_i >= @max_long_tx
        active_time = $1
        i += 1
        # Quit if the line starts with item delimiter ---
        while (lines[i] =~ /^---/) == nil
          if lines[i] =~ /MySQL thread id (\d*).* (\w*)$/
            @connection.query("KILL QUERY #{$1}")
            @logger.info"Kill long transaction: user:#{$2} thread: #{$1} active_time:#{active_time}"
          end
          i +=1
        end
      else
        i += 1
      end
    end
  rescue => e
    @logger.error("Error during kill long tx: #{e}")
  end

  def provision(plan)
    provisioned_service = ProvisionedService.new
    provisioned_service.name = "d-#{UUIDTools::UUID.random_create.to_s}".gsub(/-/, '')
    provisioned_service.user = 'u' + generate_credential
    provisioned_service.password = 'p' + generate_credential
    provisioned_service.plan = plan

    create_database(provisioned_service)

    if not provisioned_service.save then
      raise "Could not save entry: #{provisioned_service.errors.pretty_inspect}"
    end
    response = gen_credential(provisioned_service.name, provisioned_service.user, provisioned_service.password)
    return response
  rescue => e
    @logger.warn("Error during provision #{e}")
    delete_database(provisioned_service)
    return nil
  end

  def unprovision(name, credentials)
    @logger.debug("Unprovision database:#{name}, bindings: #{credentials.inspect}")
    provisioned_service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if provisioned_service.nil?
    delete_database(provisioned_service)
    # TODO: validate that database files are not lingering
    # TODO: validate remain accounts for unprovisioned database
    storage = storage_for_service(provisioned_service)
    @available_storage += storage
    # Delete all bindings
    credentials.each{ |credential| unbind(credential)} if credentials
    raise "Could not delete service: #{provisioned_service.errors.pretty_inspect}" unless provisioned_service.destroy
    @logger.debug("Successfully fulfilled unprovision request: #{name}")
  rescue => e
    @logger.warn(e)
  end

  def bind(name, bind_opts)
    @logger.debug("Bind service for db:#{name}, bind_opts = #{bind_opts}")
    binding = nil
    begin
      service = ProvisionedService.get(name)
      raise "Could not find service: #{name}" if service.nil?
      # create new credential for binding
      binding = Hash.new
      binding[:user] = 'u' + generate_credential
      binding[:password ]= 'p' + generate_credential
      binding[:bind_opts] = bind_opts
      create_database_user(name, binding[:user], binding[:password])
      response = gen_credential(name, binding[:user], binding[:password])
      @logger.debug("Bind response: #{response.inspect}")
      return response
    rescue => e
      @logger.error("Can't bind service for db:#{name} with options: #{bind_opts}: #{e}")
      delete_database_user(binding[:user]) if binding
      nil
    end
  end

  def unbind(credential)
    @logger.debug("Unbind service: #{credential.inspect}")
    name, user, bind_opts = %w(name user bind_opts).map{|k| credential[k]}
    service = ProvisionedService.get(name)
    raise "Can't find service: #{name}" unless service
    #TODO validate the existence of credential, in case we delete a normal user according to malformed credential
    delete_database_user(user)
    true
  rescue => e
   @logger.error("Can't unbind service for db:#{name}, user: #{user}, with options: #{bind_opts}: #{e}")
   nil
  end

  def create_database(provisioned_service)
    name, password, user = [:name, :password, :user].map { |field| provisioned_service.send(field) }
    begin
      start = Time.now
      @logger.debug("Creating: #{provisioned_service.pretty_inspect}")
      @connection.query("CREATE DATABASE #{name}")
      create_database_user(name, user, password)
      storage = storage_for_service(provisioned_service)
      @available_storage -= storage
      @logger.debug("Done creating #{provisioned_service.pretty_inspect}. Took #{Time.now - start}.")
    rescue Mysql::Error => e
      @logger.warn("Could not create database: [#{e.errno}] #{e.error}")
    end
  end

  def create_database_user(name, user, password)
      @logger.info("Creating credentials: #{user}/#{password} for database #{name}")
      @connection.query("GRANT ALL ON #{name}.* to #{user}@'%' IDENTIFIED BY '#{password}'")
      @connection.query("GRANT ALL ON #{name}.* to #{user}@'localhost' IDENTIFIED BY '#{password}'")
      @connection.query("FLUSH PRIVILEGES")
  end

  def delete_database(provisioned_service)
    name, user = [:name, :user].map { |field| provisioned_service.send(field) }
    begin
      @logger.info("Deleting database: #{name}")
      @connection.query("DROP DATABASE #{name}")
      delete_database_user(user)
    rescue Mysql::Error => e
      @logger.fatal("Could not delete database: [#{e.errno}] #{e.error}")
    end
  end

  def delete_database_user(user)
    @logger.info("Delete user #{user}")
    process_list = @connection.list_processes
    process_list.each do |proc|
      thread_id, user_, _, db, command, time, _, info = proc
      if user_ == user then
        @connection.query("KILL #{thread_id}")
        @logger.info("Kill session: user:#{user} db:#{db}")
      end
    end
    @connection.query("DROP USER #{user}")
    @connection.query("DROP USER #{user}@'localhost'")
  rescue Mysql::Error => e
    @logger.fatal("Could not delete user: [#{e.errno}] #{e.error}")
  end

  def gen_credential(name, user, passwd)
    response = {
      "name" => name,
      "hostname" => @local_ip,
      "port" => @mysql_config['port'],
      "user" => user,
      "password" => passwd,
    }
  end
end
