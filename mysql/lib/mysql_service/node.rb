# Copyright (c) 2009-2011 VMware, Inc.
require "erb"
require "fileutils"
require "logger"
require "pp"
#require "set"

require "datamapper"
require "nats/client"
require "uuidtools"
require "mysql"

require 'vcap/common'
require 'vcap/component'

$:.unshift(File.dirname(__FILE__))

require "mysql_service/util"
require "mysql_service/storage_quota"

module VCAP; module Services; module Mysql; end; end; end

class VCAP::Services::Mysql::Node

  KEEP_ALIVE_INTERVAL = 15
  LONG_QUERY_INTERVAL = 1
  STORAGE_QUOTA_INTERVAL = 1

  include VCAP::Services::Mysql::Util

  class ProvisionedService
    include DataMapper::Resource
    property :name,       String,   :key => true
    property :user,       String,   :required => true
    property :password,   String,   :required => true
    property :plan,       Enum[:free], :required => true
    property :quota_exceeded,  Boolean, :default => false
  end

  def initialize(options)
    @logger = options[:logger]
    @logger.info("Starting Mysql-Service-Node..")

    @local_ip = VCAP.local_ip(options[:ip_route])

    @node_id = options[:node_id]
    @mysql_config = options[:mysql]

    @max_db_size = options[:max_db_size] * 1024 * 1024
    @max_long_query = options[:max_long_query]

    @connection = mysql_connect

    EM.add_periodic_timer(KEEP_ALIVE_INTERVAL) {mysql_keep_alive}
    EM.add_periodic_timer(LONG_QUERY_INTERVAL) {kill_long_queries}
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

    @pids = {}

    @nats = NATS.connect(:uri => options[:mbus]) {on_connect}

    VCAP::Component.register(:nats => @nats,
                            :type => 'Mysql-Service-Node',
                            :host => @local_ip,
                            :config => options)

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

  def shutdown
    @logger.info("Shutting down..")
    @nats.close
  end

  def on_connect
    @logger.debug("Connected to mbus..")
    @nats.subscribe("MyaaS.provision.#{@node_id}") {|msg, reply| on_provision(msg, reply)}
    @nats.subscribe("MyaaS.unprovision.#{@node_id}") {|msg, reply| on_unprovision(msg, reply)}
    @nats.subscribe("MyaaS.unprovision") {|msg, reply| on_unprovision(msg, reply)}
    @nats.subscribe("MyaaS.discover") {|_, reply| send_node_announcement(reply)}
    send_node_announcement
    EM.add_periodic_timer(30) {send_node_announcement}
  end

  def on_provision(msg, reply)
    @logger.debug("Provision request: #{msg} from #{reply}")
    provision_message = Yajl::Parser.parse(msg)

    provisioned_service = ProvisionedService.new
    provisioned_service.name = "d-#{UUIDTools::UUID.random_create.to_s}".gsub(/-/, '')
    provisioned_service.user = 'u' + generate_credential
    provisioned_service.password = 'p' + generate_credential
    provisioned_service.plan = provision_message["plan"]

    create_database(provisioned_service)

    if not provisioned_service.save then
      delete_database(provisioned_service)
      raise "Could not save entry: #{provisioned_service.errors.pretty_inspect}"
    end

    response = {
      "node_id" => @node_id,
      "hostname" => @local_ip,
      "port" => @mysql_config['port'],
      "password" => provisioned_service.password,
      "name" => provisioned_service.name,
      "user" => provisioned_service.user
    }
    @nats.publish(reply, Yajl::Encoder.encode(response))
    @logger.debug("Successfully provisioned database for request #{msg}: #{response.inspect}")
  rescue => e
    @logger.warn(e)
  end

  def on_unprovision(msg, reply)
    @logger.debug("Unprovision request: #{msg}.")
    unprovision_message = Yajl::Parser.parse(msg)

    provisioned_service = ProvisionedService.get(unprovision_message["name"])
    raise "Could not find service: #{unprovision_message["name"]}" if provisioned_service.nil?

    delete_database(provisioned_service)

    # TODO: validate that database files are not lingering
    # TODO: kill active sessions before removing database
    storage = storage_for_service(provisioned_service)
    @available_storage += storage

    raise "Could not delete service: #{provisioned_service.errors.pretty_inspect}" unless provisioned_service.destroy
    @logger.debug("Successfully fulfilled unprovision request: #{msg}.")
  rescue => e
    @logger.warn(e)
  end

  def send_node_announcement(reply = nil)
    @logger.debug("Sending announcement for #{reply || "everyone"}")
    response = {
      :id => @node_id,
      :available_storage => @available_storage
    }
    @nats.publish(reply || "MyaaS.announce", Yajl::Encoder.encode(response))
  end

  def create_database(provisioned_service)
    name, password, user = [:name, :password, :user].map { |field| provisioned_service.send(field) }
    begin
      start = Time.now
      @logger.debug("Creating: #{provisioned_service.pretty_inspect}")
      @connection.query("CREATE DATABASE #{name}")
      @logger.info("Creating credentials: #{user}/#{password}")
      @connection.query("GRANT ALL ON #{name}.* to #{user}@'%' IDENTIFIED BY '#{password}'")
      @connection.query("GRANT ALL ON #{name}.* to #{user}@'localhost' IDENTIFIED BY '#{password}'")
      @connection.query("FLUSH PRIVILEGES")
      storage = storage_for_service(provisioned_service)
      @available_storage -= storage
      @logger.debug("Done creating #{provisioned_service.pretty_inspect}. Took #{Time.now - start}.")
    rescue => e
      @logger.warn("Could not create database: [#{e.errno}] #{e.error}")
    end
  end

  def delete_database(provisioned_service)
    name, user = [:name, :user].map { |field| provisioned_service.send(field) }
    begin
      @logger.info("Deleting database: #{name}")
      @connection.query("DROP DATABASE #{name}")
      @connection.query("DROP USER #{user}")
      @connection.query("DROP USER #{user}@'localhost'")
    rescue Mysql::Error => e
      @logger.fatal("Could not delete database: [#{e.errno}] #{e.error}")
    end
  end

end
