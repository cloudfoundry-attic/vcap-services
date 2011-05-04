$:.unshift File.join(File.dirname(__FILE__), '.')

require "base/provisioner"
require "atmos_service/common"
require "uuidtools"
require "atmos_helper"

class VCAP::Services::Atmos::Provisioner < VCAP::Services::Base::Provisioner

  include VCAP::Services::Atmos::Common

  def to_s
    @logger.debug ("VCAP::Services::Atmos::Provisioner instance: #{@aux.pretty_inspect}")
  end

  def initialize(options)
    super(options)
    @aux = options[:aux]
    @logger.debug "aux: " + @aux.to_s

    @host = @aux[:atmos_host]
    @port = @aux[:atmos_port]

    @atmos_helper = VCAP::Services::Atmos::Helper.new(@aux, @logger)
  end

  def provision_service(version, plan, &blk)
    @logger.debug("[#{service_description}: trying to create subtenant, (version=#{version}, plan=#{plan})")
    st_name = UUIDTools::UUID.random_create.to_s
    st_id = @atmos_helper.createSubtenant(st_name)

    # should we create subtenant admin rather than uid here?
    token = UUIDTools::UUID.random_create.to_s
    shared_secret = @atmos_helper.createUser(token, st_name)

    if (shared_secret == nil)
      raise "atmos create user error"
    end

    svc = {
      :data => {:subtenant_name => st_name, :subtenant_id => st_id, :host => @host},
      :service_id => st_name,
      :credentials => {:host => @host, :port => @port, :token => token, :shared_secret => shared_secret, :subtenant_id => st_id}
    }
    @logger.debug("Service provisioned: #{svc.pretty_inspect}")
    @prov_svcs[svc[:service_id]] = svc
    blk.call(svc)
  rescue => e
    @logger.warn(e)
    blk.call(nil)
  end

  def unprovision_service(instance_id, &blk)
    begin
      success = @atmos_helper.deleteSubtenant(instance_id)
      if success == true
        bindings = find_all_bindings(instance_id)
        @logger.debug("unprovision service: #{instance_id} ")
        @prov_svcs.delete(instance_id)
        bindings.each do |b|
          @logger.debug("delete binded user: #{b[:service_id]} ")
          @prov_svcs.delete(b[:service_id])
        end
      end
    rescue => e
      @logger.warn(e)
      success = nil
    end
    blk.call(success)
  end

  def bind_instance(instance_id, binding_options, &blk)
    @logger.debug("try to bind service: #{instance_id}")
    if instance_id.nil?
      @logger.warn("#{instance_id} is null!")
      blk.call(nil)
    end

    begin
      svc = @prov_svcs[instance_id]
      raise "#{instance_id} not found!" if svc.nil?
      @logger.debug("svc[data]: #{svc[:data]}")

      token = UUIDTools::UUID.random_create.to_s
      shared_secret = @atmos_helper.createUser(token, instance_id)

      if (shared_secret == nil)
        raise "atmos create user error"
      end

      res = {
        :service_id => token,
        :configuration => svc[:data],
        :credentials => {:host => @host, :port => @port, :token => token, :shared_secret => shared_secret, :subtenant_id => svc[:data][:subtenant_id]}
      }
      @logger.debug("binded: #{res.pretty_inspect}")
      @prov_svcs[res[:service_id]] = res
      blk.call(res)
    rescue => e
      @logger.warn(e)
      blk.call(nil)
    end
  end

  def unbind_instance(instance_id, handle_id, binding_options, &blk)
    begin
      raise "instance_id cannot be nil" if instance_id.nil?
      svc = @prov_svcs[handle_id]
      raise "#{handle_id} not found!" if svc.nil?

      configuration = (svc[:configuration].nil?) ? svc[:data] : svc[:configuration]
      @logger.debug("svc[configuration]: #{configuration}")
      success = @atmos_helper.deleteUser(handle_id, instance_id)
      @prov_svcs.delete(handle_id) if success == true
    rescue => e
      @logger.warn(e)
      success = nil
    end
    blk.call(success)
  end

end

