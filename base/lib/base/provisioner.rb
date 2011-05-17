# Copyright (c) 2009-2011 VMware, Inc.
require "pp"
require "set"
require "datamapper"
require "uuidtools"

$LOAD_PATH.unshift File.dirname(__FILE__)
require 'base/base'
require 'barrier'

class VCAP::Services::Base::Provisioner < VCAP::Services::Base::Base
  MASKED_PASSWORD = '********'

  def initialize(options)
    super(options)
    @version   = options[:version]
    @node_timeout = options[:node_timeout] || 2
    @nodes     = {}
    @prov_svcs = {}
    EM.add_periodic_timer(60) { process_nodes }
  end

  def flavor
    'Provisioner'
  end

  # Updates our internal state to match that supplied by handles
  # +handles+  An array of config handles
  def update_handles(handles)
    @logger.debug("Update handles: #{handles.inspect}")
    handles.each do |handle|
      h = handle.deep_dup
      @prov_svcs[h['service_id']] = {
        :configuration => h['configuration'],
        :credentials => h['credentials'],
        :service_id => h['service_id']
      }
    end
    @logger.debug("[#{service_description}] Handles updated prov_svcs: #{@prov_svcs}")
  end

  def find_all_bindings(name)
    res = []
    @prov_svcs.each do |k,v|
      res << v[:credentials] if v[:credentials]["name"] == name && v[:service_id] != name
    end
    res
  end

  def process_nodes
    @nodes.delete_if {|_, timestamp| Time.now.to_i - timestamp > 300}
  end

  def on_connect_node
    @logger.debug("[#{service_description}] Connected to node mbus..")
    @node_nats.subscribe("#{service_name}.announce") { |msg|
      on_node_announce(msg)
    }
    @node_nats.subscribe("#{service_name}.handles") {|msg, reply| on_query_handles(msg, reply) }
    @node_nats.subscribe("#{service_name}.update_service_handle") {|msg, reply| on_update_service_handle(msg, reply) }
    @node_nats.publish("#{service_name}.discover")
  end

  def on_node_announce(msg)
    @logger.debug("[#{service_description}] Received node announcement: #{msg}")
    announce_message = Yajl::Parser.parse(msg)
    @nodes[announce_message["id"]] = Time.now.to_i if announce_message["id"]
  end

  # query all handles for a given instance
  def on_query_handles(instance, reply)
    @logger.debug("[#{service_description}] Receive query handles request for instance: #{instance}")
    if instance.empty?
      res = Yajl::Encoder.encode(@prov_svcs)
    else
      handles = find_all_bindings(msg)
      res = Yajl::Encoder.encode(handles)
    end
    @node_nats.publish(reply, res)
  end

  def unprovision_service(instance_id, &blk)
    @logger.debug("[#{service_description}] Unprovision service #{instance_id}")
    begin
      svc = @prov_svcs[instance_id]
      raise ServiceError.new(ServiceError::NOT_FOUND, "instance_id #{instance_id}") if svc.nil?

      node_id = svc[:credentials]["node_id"]
      raise "Cannot find node_id for #{instance_id}" if node_id.nil?

      bindings = find_all_bindings(instance_id)
      @logger.debug("[#{service_description}] Unprovisioning instance #{instance_id} from #{node_id}")
      request = {
        'name'     => instance_id,
        'bindings' => bindings
      }

      @logger.debug("[#{service_description}] Sending reqeust #{request}")
      subscription = nil
      timer = EM.add_timer(@node_timeout) {
        @node_nats.unsubscribe(subscription)
        blk.call(timeout_fail)
      }
      subscription =
        @node_nats.request(
          "#{service_name}.unprovision.#{node_id}",
          Yajl::Encoder.encode(request)
       ) do |msg|
          # Delete local entries
          @prov_svcs.delete(instance_id)
          bindings.each do |b|
            @prov_svcs.delete(b[:service_id])
          end

          EM.cancel_timer(timer)
          @node_nats.unsubscribe(subscription)
          opts = Yajl::Parser.parse(msg)
          blk.call(opts)
        end
    rescue => e
      if e.instance_of? ServiceError
        blk.call(failure(e))
      else
        @logger.warn(e)
        blk.call(internal_fail)
      end
    end
  end

  def provision_service(request, prov_handle=nil, &blk)
    @logger.debug("[#{service_description}] Attempting to provision instance (label=#{request['label']}, plan=#{request['plan']})")
    subscription = nil
    barrier = VCAP::Services::Base::Barrier.new(:timeout => @node_timeout, :callbacks => @nodes.length) do |responses|
      @logger.debug("[#{service_description}] Found the following nodes: #{responses.pretty_inspect}")
      @node_nats.unsubscribe(subscription)
      unless responses.empty?
        provision_node(request, responses, prov_handle, blk)
      end
    end
    subscription = @node_nats.request("#{service_name}.discover", &barrier.callback)
  rescue => e
    @logger.warn(e)
    blk.call(internal_fail)
  end

  def provision_node(request, node_msgs, prov_handle, blk)
    @logger.debug("[#{service_description}] Provisioning node (label=#{request['label']}, plan=#{request['plan']}, nnodes=#{node_msgs.length})")
    nodes = node_msgs.map { |msg| Yajl::Parser.parse(msg.first) }
    best_node = nodes.max_by { |node| node_score(node) }
    if best_node && node_score(best_node) > 0
      best_node = best_node["id"]
      @logger.debug("[#{service_description}] Provisioning on #{best_node}")
      request = {"plan" => request['plan']}
      # use old credentials to provision a service if provided.
      request["credentials"] = prov_handle["credentials"] if prov_handle
      subscription = nil
      timer = EM.add_timer(@node_timeout) {
        @node_nats.unsubscribe(subscription)
        blk.call(timeout_fail)
      }
      subscription =
        @node_nats.request(
          "#{service_name}.provision.#{best_node}",
          Yajl::Encoder.encode(request)
       ) do |msg|
          EM.cancel_timer(timer)
          @node_nats.unsubscribe(subscription)
          opts = Yajl::Parser.parse(msg)
          if opts['success']
            opts = opts['response']
            # remove unnecessary credential in request
            request.delete('credentials') if request.has_key?('credentials')
            svc = {:data => request, :service_id => opts['name'], :credentials => opts}
            # FIXME: workaround for inconsistant representation of bind handle and provision handle
            svc_local = {:configuration => request, :service_id => opts['name'], :credentials => opts}
            @logger.debug("Provisioned #{svc.pretty_inspect}")
            @prov_svcs[svc[:service_id]] = svc_local
            blk.call(success(svc))
          else
            blk.call(opts)
          end
        end
    else
      # No resources
      @logger.warn("[#{service_description}] Could not find a node to provision")
      blk.call(internal_fail)
    end
  end

  def bind_instance(instance_id, binding_options, bind_handle=nil, &blk)
    @logger.debug("[#{service_description}] Attempting to bind to service #{instance_id}")

    begin
      svc = @prov_svcs[instance_id]
      raise ServiceError.new(ServiceError::NOT_FOUND, instance_id) if svc.nil?

      node_id = svc[:credentials]["node_id"]
      raise "Cannot find node_id for #{instance_id}" if node_id.nil?

      @logger.debug("[#{service_description}] bind instance #{instance_id} from #{node_id}")
      #FIXME options = {} currently, should parse it in future.
      request = {
        'name'      => instance_id,
        'bind_opts' => binding_options
      }
      request["credentials"] = bind_handle["credentials"] if bind_handle
      subscription = nil
      timer = EM.add_timer(@node_timeout) {
        @node_nats.unsubscribe(subscription)
        blk.call(timeout_fail)
      }
      subscription =
        @node_nats.request( "#{service_name}.bind.#{node_id}",
          Yajl::Encoder.encode(request)
       ) do |msg|
          EM.cancel_timer(timer)
          @node_nats.unsubscribe(subscription)
          opts = Yajl::Parser.parse(msg)
          if(opts['success'])
            opts = opts['response']
            res = {
              :service_id => UUIDTools::UUID.random_create.to_s,
              :configuration => svc[:configuration],
              :credentials => opts
            }
            @logger.debug("[#{service_description}] Binded: #{res.pretty_inspect}")
            @prov_svcs[res[:service_id]] = res
            blk.call(success(res))
          else
            blk.call(opts)
          end
        end
    rescue => e
      if e.instance_of? ServiceError
        blk.call(failure(e))
      else
        @logger.warn(e)
        blk.call(internal_fail)
      end
    end
  end

  def unbind_instance(instance_id, handle_id, binding_options, &blk)
    @logger.debug("[#{service_description}] Attempting to unbind to service #{instance_id}")
    begin
      svc = @prov_svcs[instance_id]
      raise ServiceError.new(ServiceError::NOT_FOUND, "instance_id #{instance_id}") if svc.nil?

      handle = @prov_svcs[handle_id]
      raise ServiceError.new(ServiceError::NOT_FOUND, "handle_id #{handle_id}") if handle.nil?

      node_id = svc[:credentials]["node_id"]
      raise "Cannot find node_id for #{instance_id}" if node_id.nil?

      @logger.debug("[#{service_description}] Unbind instance #{handle_id} from #{node_id}")
      request = handle[:credentials]

      subscription = nil
      timer = EM.add_timer(@node_timeout) {
        @node_nats.unsubscribe(subscription)
        blk.call(timeout_fail)
      }
      subscription =
        @node_nats.request( "#{service_name}.unbind.#{node_id}",
          Yajl::Encoder.encode(request)
       ) do |msg|
          EM.cancel_timer(timer)
          @node_nats.unsubscribe(subscription)
          opts = Yajl::Parser.parse(msg)
          @prov_svcs.delete(handle_id)
          blk.call(opts)
        end
    rescue => e
      if e.instance_of? ServiceError
        blk.call(failure(e))
      else
        @logger.warn(e)
        blk.call(internal_fail)
      end
    end
  end

  def restore_instance(instance_id, backup_path, &blk)
    @logger.debug("[#{service_description}] Attempting to restore to service #{instance_id}")

    begin
      svc = @prov_svcs[instance_id]
      raise ServiceError.new(ServiceError::NOT_FOUND, instance_id) if svc.nil?

      node_id = svc[:credentials]["node_id"]
      raise "Cannot find node_id for #{instance_id}" if node_id.nil?

      @logger.debug("[#{service_description}] restore instance #{instance_id} from #{node_id}")
      request = {
        'instance_id' => instance_id,
        'backup_path' => backup_path
      }

      subscription = nil
      timer = EM.add_timer(@node_timeout) {
        @node_nats.unsubscribe(subscription)
        blk.call(timeout_fail)
      }
      subscription =
        @node_nats.request( "#{service_name}.restore.#{node_id}",
          Yajl::Encoder.encode(request)
       ) do |msg|
          EM.cancel_timer(timer)
          @node_nats.unsubscribe(subscription)
          opts = Yajl::Parser.parse(msg)
          blk.call(opts)
        end
    rescue => e
      if e.instance_of? ServiceError
        blk.call(failure(e))
      else
        @logger.warn(e)
        blk.call(internal_fail)
      end
    end
  end

  # Recover an instance
  # 1) Provision an instance use old credential
  # 2) restore instance use backup file
  # 3) re-bind bindings use old credential
  def recover(instance_id, backup_path, handles, &blk)
    @logger.debug("Recover instance: #{instance_id} form #{backup_path} with handles #{handles.inspect}.")
    prov_handle, binding_handles = find_instance_handles(instance_id, handles)
    @logger.debug("Provsion Handle: #{prov_handle.inspect}")
    request = prov_handle["configuration"]
    provision_service(request, prov_handle) do |msg|
      if msg['success']
        @logger.info("Recover: Success re-provision instance.")
        restore_instance(instance_id, backup_path) do |res|
          if res['success']
            @logger.info("Recover: Success restore instance.")
            binding_handles.each do |handle|
              bind_instance(instance_id, nil, handle) do |bind_res|
                if bind_res['success']
                  @logger.info("Recover: Success re-bind bindings.")
                else
                  blk.call(internal_fail)
                end
              end
            end
            success = {
              'success' => true,
              'response' => "{}"
            }
            blk.call(success)
          else
            blk.call(internal_fail)
          end
        end
      else
        blk.call(internal_fail)
      end
    end
  end

  def on_update_service_handle(msg, reply)
    @logger.debug("[#{service_description}] Update service handle #{msg.inspect}")
    handle = Yajl::Parser.parse(msg)
    @update_handle_callback.call(handle) do |response|
      response = Yajl::Encoder.encode(response)
      @node_nats.publish(reply, response)
    end
  end
  def on_update_service_handle(msg, reply)
    @logger.debug("[#{service_description}] Update service handle #{msg.inspect}")
    handle = Yajl::Parser.parse(msg)
    @update_handle_callback.call(handle) do |response|
      response = Yajl::Encoder.encode(response)
      @node_nats.publish(reply, response)
    end
  end

  def register_update_handle_callback(&blk)
    @logger.debug("Register update handle callback with #{blk}")
    @update_handle_callback = blk
  end

  def varz_details()
    # Service Provisioner subclasses may want to override this method
    # to provide service specific data beyond the following

    # Mask password from varz details
    svcs = @prov_svcs.deep_dup
    svcs.each do |k,v|
      v[:credentials]['pass'] &&= MASKED_PASSWORD
      v[:credentials]['password'] &&= MASKED_PASSWORD
    end

    varz = {
      :nodes => @nodes,
      :prov_svcs => svcs
    }
    return varz
  rescue => e
    @logger.warn(e)
  end

  ########
  # Helpers
  ########

  # Find instance related handles in all handles
  def find_instance_handles(instance_id, handles)
    prov_handle = nil
    binding_handles = []
    handles.each do |h|
      if h['service_id'] == instance_id
        prov_handle = h
      else
        binding_handles << h if h['configuration']['name'] == instance_id
      end
    end
    return [prov_handle, binding_handles]
  end

  # Service Provisioner subclasses must implement the following
  # methods

  # node_score(node) -> number.  this base class provisions on the
  # "best" node (lowest load, most free capacity, etc). this method
  # should return a number; higher scores represent "better" nodes;
  # negative/zero scores mean that a node should be ignored
  abstract :node_score

  # service_name() --> string
  # (inhereted from VCAP::Services::Base::Base)

end
