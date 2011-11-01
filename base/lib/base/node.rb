# Copyright (c) 2009-2011 VMware, Inc.
require 'nats/client'
require 'vcap/component'
require 'fileutils'

$:.unshift(File.dirname(__FILE__))
require 'base'
require 'service_message'

class VCAP::Services::Base::Node < VCAP::Services::Base::Base
  include VCAP::Services::Internal

  def initialize(options)
    super(options)
    @node_id = options[:node_id]
    @migration_nfs = options[:migration_nfs]
  end

  def flavor
    return "Node"
  end

  def on_connect_node
    @logger.debug("#{service_description}: Connected to node mbus")
    @node_nats.subscribe("#{service_name}.provision.#{@node_id}") { |msg, reply|
      EM.defer { on_provision(msg, reply) }
    }
    @node_nats.subscribe("#{service_name}.unprovision.#{@node_id}") { |msg, reply|
      EM.defer { on_unprovision(msg, reply) }
    }
    @node_nats.subscribe("#{service_name}.bind.#{@node_id}") { |msg, reply|
      EM.defer { on_bind(msg, reply) }
    }
    @node_nats.subscribe("#{service_name}.unbind.#{@node_id}") { |msg, reply|
      EM.defer { on_unbind(msg, reply) }
    }
    @node_nats.subscribe("#{service_name}.restore.#{@node_id}") { |msg, reply|
      EM.defer { on_restore(msg, reply) }
    }
    @node_nats.subscribe("#{service_name}.discover") { |_, reply|
      on_discover(reply)
    }
    # rebalance channels
    @node_nats.subscribe("#{service_name}.disable_instance.#{@node_id}") { |msg, reply|
      on_disable_instance(msg, reply)
    }
    @node_nats.subscribe("#{service_name}.enable_instance.#{@node_id}") { |msg, reply|
      on_enable_instance(msg, reply)
    }
    @node_nats.subscribe("#{service_name}.import_instance.#{@node_id}") { |msg, reply|
      on_import_instance(msg, reply)
    }
    @node_nats.subscribe("#{service_name}.cleanup_nfs.#{@node_id}") { |msg, reply|
      on_cleanup_nfs(msg, reply)
    }
    #Purge orphan
    @node_nats.subscribe("#{service_name}.check_orphan") { |msg, reply|
      on_check_orphan(msg, reply)
    }
    @node_nats.subscribe("#{service_name}.purge_orphan.#{@node_id}") { |msg, reply|
      on_purge_orphan(msg, reply)
    }
    pre_send_announcement
    send_node_announcement
    EM.add_periodic_timer(30) {
      send_node_announcement
    }
  end

  def on_provision(msg, reply)
    @logger.debug("#{service_description}: Provision request: #{msg} from #{reply}")
    response = ProvisionResponse.new
    provision_req = ProvisionRequest.decode(msg)
    plan = provision_req.plan
    credentials = provision_req.credentials
    credential = provision(plan, credentials)
    credential['node_id'] = @node_id
    response.credentials = credential
    @logger.debug("#{service_description}: Successfully provisioned service for request #{msg}: #{response.inspect}")
    @node_nats.publish(reply, encode_success(response))
  rescue => e
    @logger.warn(e)
    @node_nats.publish(reply, encode_failure(response, e))
  end

  def on_unprovision(msg, reply)
    @logger.debug("#{service_description}: Unprovision request: #{msg}.")
    response = SimpleResponse.new
    unprovision_req = UnprovisionRequest.decode(msg)
    name     = unprovision_req.name
    bindings = unprovision_req.bindings
    result = unprovision(name, bindings)
    if result
      @node_nats.publish(reply, encode_success(response))
    else
      @node_nats.publish(reply, encode_failure(response))
    end
  rescue => e
    @logger.warn(e)
    @node_nats.publish(reply, encode_failure(response, e))
  end

  def on_bind(msg, reply)
    @logger.debug("#{service_description}: Bind request: #{msg} from #{reply}")
    response = BindResponse.new
    bind_message = BindRequest.decode(msg)
    name      = bind_message.name
    bind_opts = bind_message.bind_opts
    credentials = bind_message.credentials
    response.credentials = bind(name, bind_opts, credentials)
    @node_nats.publish(reply, encode_success(response))
  rescue => e
    @logger.warn(e)
    @node_nats.publish(reply, encode_failure(response, e))
  end

  def on_unbind(msg, reply)
    @logger.debug("#{service_description}: Unbind request: #{msg} from #{reply}")
    response = SimpleResponse.new
    unbind_req = UnbindRequest.decode(msg)
    result = unbind(unbind_req.credentials)
    if result
      @node_nats.publish(reply, encode_success(response))
    else
      @node_nats.publish(reply, encode_failure(response))
    end
  rescue => e
    @logger.warn(e)
    @node_nats.publish(reply, encode_failure(response, e))
  end

  def on_restore(msg, reply)
    @logger.debug("#{service_description}: Restore request: #{msg} from #{reply}")
    response = SimpleResponse.new
    restore_message = RestoreRequest.decode(msg)
    instance_id = restore_message.instance_id
    backup_path = restore_message.backup_path
    result = restore(instance_id, backup_path)
    if result
      @node_nats.publish(reply, encode_success(response))
    else
      @node_nats.publish(reply, encode_failure(response))
    end
  rescue => e
    @logger.warn(e)
    @node_nats.publish(reply, encode_failure(response, e))
  end

  # disable and dump instance
  def on_disable_instance(msg, reply)
    @logger.debug("#{service_description}: Disable instance #{msg} request from #{reply}")
    credentials = Yajl::Parser.parse(msg)
    prov_cred, binding_creds = credentials
    instance = prov_cred['name']
    file_path = get_migration_folder(instance)
    FileUtils.mkdir_p(file_path)
    result = disable_instance(prov_cred, binding_creds)
    result = dump_instance(prov_cred, binding_creds, file_path) if result
    @node_nats.publish(reply, Yajl::Encoder.encode(result))
  rescue => e
    @logger.warn(e)
  end

  # enable instance and send updated credentials back
  def on_enable_instance(msg, reply)
    @logger.debug("#{service_description}: enable instance #{msg} request from #{reply}")
    credentials = Yajl::Parser.parse(msg)
    prov_cred, binding_creds_hash = credentials
    result = enable_instance(prov_cred, binding_creds_hash)
    # Update node_id in provision credentials..
    prov_cred, binding_creds_hash = result
    prov_cred['node_id'] = @node_id
    result = [prov_cred, binding_creds_hash]
    @node_nats.publish(reply, Yajl::Encoder.encode(result))
  rescue => e
    @logger.warn(e)
  end

  # Cleanup nfs folder which contains migration data
  def on_cleanup_nfs(msg, reply)
    @logger.debug("#{service_description}: cleanup nfs request #{msg} from #{reply}")
    request = Yajl::Parser.parse(msg)
    prov_cred, binding_creds = request
    instance = prov_cred['name']
    FileUtils.rm_rf(get_migration_folder(instance))
    @node_nats.publish(reply, Yajl::Encoder.encode(true))
  rescue => e
    @logger.warn(e)
  end

  def on_check_orphan(msg, reply)
    @logger.debug("#{service_description}: handles for checking orphan " )
    response = CheckOrphanResponse.new
    request = CheckOrphanRequest.decode(msg)
    check_orphan(request.handles)
    response.orphan_instances = @orphan_ins_hash
    response.orphan_bindings = @orphan_binding_hash
    response.success = true
  rescue => e
    @logger.warn("Exception at on_check_orphan #{e}")
    response.success = false
    response.error = e
  ensure
    @node_nats.publish("#{service_name}.orphan_result", response.encode) if response
  end

  def check_orphan(handles)
    raise ServiceError.new(ServiceError::NOT_FOUND, "No handles for checking orphan") if handles.nil?

    live_ins_list = all_instances_list
    orphan_ins_hash = {}
    oi_list = []
    live_ins_list.each do |name|
      oi_list << name unless handles.index {|h| h["credentials"]["node_id"] == @node_id && h["service_id"] == name }
    end

    live_bind_list = all_bindings_list
    orphan_binding_hash = {}
    ob_list = []
    live_bind_list.each do |credential|
      ob_list << credential unless handles.index{|h| h["credentials"]["name"] == credential["name"] && h["credentials"]["username"] == credential["username"]}
    end

    @logger.debug("Orphan Instances: #{oi_list.count};  Orphan Bindings: #{ob_list.count}")
    orphan_ins_hash["#{@node_id}"] = oi_list
    orphan_binding_hash["#{@node_id}"] = ob_list
    @orphan_ins_hash = orphan_ins_hash
    @orphan_binding_hash = orphan_binding_hash
  end

  def on_purge_orphan(msg, reply)
    @logger.debug("#{service_description}: Message for purging orphan " )
    response = SimpleResponse.new
    request = PurgeOrphanRequest.decode(msg)
    result = purge_orphan(request.orphan_ins_list,request.orphan_binding_list)
    if result
      @node_nats.publish(reply, encode_success(response))
    else
      @node_nats.publish(reply, encode_failure(response))
    end
  rescue => e
    @logger.warn(e)
    @node_nats.publish(reply, encode_failure(response, e))
  end

  def purge_orphan(oi_list,ob_list)
    ret = true
    ab_list = all_bindings_list
    oi_list.each do |ins|
      begin
        bindings = ab_list.select {|b| b["name"] == ins}
        @logger.debug("Unprovision orphan instance #{ins} and its #{bindings.size} bindings")
        ret &&= unprovision(ins,bindings)
        # Remove the OBs that are unbinded by unprovision
        ob_list.delete_if do |ob|
          bindings.index do |binding|
            binding["username"] == ob["username"]
          end
        end
      rescue => e
        @logger.debug("Error on purge orphan instance #{ins}: #{e}")
      end
    end

    ob_list.each do |credential|
      begin
        @logger.debug("Unbind orphan binding #{credential}")
        ret &&= unbind(credential)
      rescue => e
        @logger.debug("Error on purge orphan binding #{credential}: #{e}")
      end
    end
    ret
  end

  # Subclass must overwrite this method to enable check orphan instance feature.
  # Otherwise it will not check orphan instance
  # The return value should be a list of instance name(handle["service_id"]).
  def all_instances_list
    []
  end

  # Subclass must overwrite this method to enable check orphan binding feature.
  # Otherwise it will not check orphan bindings
  # The return value should be a list of binding credentials
  # Binding credential will be the argument for unbind method
  # And it should have at least username & name property for base code
  # to find the orphans
  def all_bindings_list
    []
  end

  # Get the tmp folder for migration
  def get_migration_folder(instance)
    File.join(@migration_nfs, 'migration', service_name, instance)
  end

  def on_import_instance(msg, reply)
    @logger.debug("#{service_description}: import instance #{msg} request from #{reply}")
    credentials = Yajl::Parser.parse(msg)
    plan, prov_cred, binding_creds = credentials
    instance = prov_cred['name']
    file_path = get_migration_folder(instance)
    result = import_instance(prov_cred, binding_creds, file_path, plan)
    @node_nats.publish(reply, Yajl::Encoder.encode(result))
  rescue => e
    @logger.warn(e)
  end

  def on_discover(reply)
    send_node_announcement(reply)
  end

  def pre_send_announcement
  end

  def send_node_announcement(reply = nil)
    unless node_ready?
      @logger.debug("#{service_description}: Not ready to send announcement")
      return
    end
    @logger.debug("#{service_description}: Sending announcement for #{reply || "everyone"}")
    a = announcement
    a[:id] = @node_id
    @node_nats.publish(reply || "#{service_name}.announce", Yajl::Encoder.encode(a))
  rescue
    @logger.warn(e)
  end

  def node_ready?()
    # Service Node subclasses can override this method if they depend
    # on some external service in order to operate; for example, MySQL
    # and Postgresql require a connection to the underlying server.
    true
  end

  def varz_details()
    # Service Node subclasses may want to override this method to
    # provide service specific data beyond what is returned by their
    # "announcement" method.
    return announcement
  end

  def healthz_details()
    # Service Node subclasses may want to override this method to
    # provide service specific data
    healthz = {
      :self => "ok"
    }
  end

  # Helper
  def encode_success(response)
    response.success = true
    response.encode
  end

  def encode_failure(response, error=nil)
    response.success = false
    if error.nil? || !error.is_a?(ServiceError)
      error = ServiceError.new(ServiceError::INTERNAL_ERROR)
    end
    response.error = error.to_hash
    response.encode
  end

  # Service Node subclasses must implement the following methods

  # provision(plan) --> {name, host, port, user, password}
  abstract :provision

  # unprovision(name) --> void
  abstract :unprovision

  # bind(name, bind_opts) --> {host, port, login, secret}
  abstract :bind

  # unbind(credentials)  --> void
  abstract :unbind

  # announcement() --> { any service-specific announcement details }
  abstract :announcement

  # service_name() --> string
  # (inhereted from VCAP::Services::Base::Base)

  # <action>_instance(prov_credential, binding_credentials)  -->  true for success and nil for fail
  abstract :disable_instance, :dump_instance, :import_instance, :enable_instance

end
