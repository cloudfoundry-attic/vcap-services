# Copyright (c) 2009-2011 VMware, Inc.
require "fileutils"
require "logger"
require "datamapper"
require "uuidtools"

module VCAP
  module Services
    module Echo
      class Node < VCAP::Services::Base::Node
      end
    end
  end
end

require "echo_service/common"
require "echo_service/echo_error"

class VCAP::Services::Echo::Node

  include VCAP::Services::Echo::Common
  include VCAP::Services::Echo

  class ProvisionedService
    include DataMapper::Resource
    property :name,       String,   :key => true
  end

  def initialize(options)
    super(options)

    @local_db = options[:local_db]
    @port = options[:port]
    @base_dir = options[:base_dir]
    @supported_versions = ["1.0"]
  end

  def pre_send_announcement
    super
    FileUtils.mkdir_p(@base_dir) if @base_dir
    start_db
    @capacity_lock.synchronize do
      ProvisionedService.all.each do |instance|
        @capacity -= capacity_unit
      end
    end
  end

  def announcement
    @capacity_lock.synchronize do
      { :available_capacity => @capacity,
        :capacity_unit => capacity_unit }
    end
  end

  def provision(plan, credential = nil, version=nil)
    instance = ProvisionedService.new
    if credential
      instance.name = credential["name"]
    else
      instance.name = UUIDTools::UUID.random_create.to_s
    end

    begin
      save_instance(instance)
    rescue => e1
      @logger.error("Could not save instance: #{instance.name}, cleanning up")
      begin
        destroy_instance(instance)
      rescue => e2
        @logger.error("Could not clean up instance: #{instance.name}")
      end
      raise e1
    end

    gen_credential(instance)
  end

  def unprovision(name, credentials = [])
    return if name.nil?
    @logger.debug("Unprovision echo service: #{name}")
    instance = get_instance(name)
    destroy_instance(instance)
    true
  end

  def bind(name, binding_options, credential = nil)
    instance = nil
    if credential
      instance = get_instance(credential["name"])
    else
      instance = get_instance(name)
    end
    gen_credential(instance)
  end

  def unbind(credential)
    @logger.debug("Unbind service: #{credential.inspect}")
    true
  end

  def start_db
    DataMapper.setup(:default, @local_db)
    DataMapper::auto_upgrade!
  end

  def save_instance(instance)
    raise EchoError.new(EchoError::ECHO_SAVE_INSTANCE_FAILED, instance.inspect) unless instance.save
  end

  def destroy_instance(instance)
    raise EchoError.new(EchoError::ECHO_DESTORY_INSTANCE_FAILED, instance.inspect) unless instance.destroy
  end

  def get_instance(name)
    instance = ProvisionedService.get(name)
    raise EchoError.new(EchoError::ECHO_FIND_INSTANCE_FAILED, name) if instance.nil?
    instance
  end

  def gen_credential(instance)
    credential = {
      "host" => get_host,
      "port" => @port,
      "name" => instance.name
    }
  end
end
