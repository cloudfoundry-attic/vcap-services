# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), '.')

require "base/provisioner"
require "atmos_service/common"
require "uuidtools"
require "atmos_helper"
require "atmos_error"

class VCAP::Services::Atmos::Provisioner < VCAP::Services::Base::Provisioner

  include VCAP::Services::Atmos::Common
  include VCAP::Services::Atmos

  ATMOS_CONFIG_FILE = File.expand_path("../../../config/atmos_gateway.yml", __FILE__)

  def to_s
    "VCAP::Services::Atmos::Provisioner instance: #{@atmos_config.inspect}"
  end

  def get_atmos_config
    config_file = YAML.load_file(ATMOS_CONFIG_FILE)
    config = VCAP.symbolize_keys(config_file)
    config[:atmos]
  end

  def initialize(options)
    super(options)
    @atmos_config = options[:additional_options][:atmos] || get_atmos_config
    @logger.debug "atmos_config: #{@atmos_config.inspect}"

    @host = @atmos_config[:host]
    @port = @atmos_config[:port]

    @atmos_helper = VCAP::Services::Atmos::Helper.new(@atmos_config, @logger)
  end

  def provision_service(request, prov_handle=nil, &blk)
    @logger.debug("[#{service_description}] Attempting to provision instance (request=#{request.extract})")
    begin
      st_name = UUIDTools::UUID.random_create.to_s
      st_id = @atmos_helper.create_subtenant(st_name)

      # should we create subtenant admin rather than uid here?
      token = UUIDTools::UUID.random_create.to_s
      shared_secret = @atmos_helper.create_user(token, st_name)

      svc = {
        :data => {:subtenant_name => st_name, :subtenant_id => st_id, :host => @host},
        :service_id => st_name,
        :credentials => {:host => @host, :port => @port, :token => token,
          :shared_secret => shared_secret, :subtenant_id => st_id}
      }
      # set 'configuration' instead of 'data' to keep local hash consistent
      svc_local = {
        :configuration => {"subtenant_name" => st_name, "subtenant_id" => st_id, "host" => @host},
        :service_id => st_name,
        :credentials => {"host" => @host, "port" => @port, "token" => token,
          "shared_secret" => shared_secret, "subtenant_id" => st_id}
      }
      @logger.debug("Service provisioned: #{svc.inspect}")
      @prov_svcs[svc[:service_id]] = svc_local
      blk.call(success(svc))
    rescue => e
      # roll back work
      @logger.warn 'provision error, trying to roll back if necessary'
      begin
        @atmos_helper.delete_subtenant(st_name) if st_id
      rescue => e1
        @logger.info 'roll back error'
      end
      if e.instance_of? AtmosError
        blk.call(failure(e))
      else
        @logger.warn(e)
        blk.call(internal_fail)
      end
    end
  end

  def unprovision_service(instance_id, &blk)
    @logger.debug("[#{service_description}] Attempting to unprovision instance (instance id=#{instance_id}")
    begin
      success = @atmos_helper.delete_subtenant(instance_id)
      if success
        bindings = find_all_bindings(instance_id)
        @logger.debug("unprovision service: #{instance_id} ")
        @prov_svcs.delete(instance_id)
        bindings.each do |b|
          @logger.debug("delete binded user: #{b[:service_id]} ")
          @prov_svcs.delete(b[:service_id])
        end
      end
      blk.call(success())
    rescue => e
      if e.instance_of? AtmosError
        blk.call(failure(e))
      else
        @logger.warn(e)
        blk.call(internal_fail)
      end
    end
  end

  def bind_instance(instance_id, binding_options, bind_handle=nil, &blk)
    @logger.debug("attempting to bind service: #{instance_id}")
    if instance_id.nil?
      @logger.warn("#{instance_id} is null!")
      blk.call(internal_fail)
    end

    begin
      svc = @prov_svcs[instance_id]
      raise "#{instance_id} not found!" if svc.nil?
      @logger.debug("svc[configuration]: #{svc[:configuration]}")

      token = UUIDTools::UUID.random_create.to_s
      shared_secret = @atmos_helper.create_user(token, instance_id)

      res = {
        :service_id => token,
        :configuration => svc[:configuration],
        :credentials => {:host => @host, :port => @port, :token => token,
          :shared_secret => shared_secret, :subtenant_id => svc[:configuration]["subtenant_id"]}
      }
      @logger.debug("binded: #{res.inspect}")
      @prov_svcs[res[:service_id]] = res
      blk.call(success(res))
    rescue => e
      if e.instance_of? AtmosError
        blk.call(failure(e))
      else
        @logger.warn(e)
        blk.call(internal_fail)
      end
    end
  end

  def unbind_instance(instance_id, handle_id, binding_options, &blk)
    @logger.debug("attempting to unbind service: #{instance_id}")
    begin
      raise "instance_id cannot be nil" if instance_id.nil?
      svc = @prov_svcs[handle_id]
      raise "#{handle_id} not found!" if svc.nil?

      @logger.debug("svc[configuration]: #{svc[configuration]}")
      success = @atmos_helper.delete_user(handle_id, instance_id)
      @prov_svcs.delete(handle_id) if success
      blk.call(success())
    rescue => e
      if e.instance_of? AtmosError
        blk.call(failure(e))
      else
        @logger.warn(e)
        blk.call(internal_fail)
      end
    end
  end

end
