# Copyright (c) 2009-2011 VMware, Inc.
require "erb"
require "fileutils"
require "logger"
require "pp"
require "set"

require "datamapper"
require "eventmachine"
require "nats/client"
require "uuidtools"

require "vcap/common"
require "vcap/component"

require "redis_service/barrier"

module VCAP
  module Services
    module Redis
    end
  end
end

class VCAP::Services::Redis::Provisioner

  def initialize(opts)
    @logger    = opts[:logger]
    @version   = opts[:version]
    @local_ip  = VCAP.local_ip(opts[:local_ip])
    @nodes     = {}
    @svc_mbus  = opts[:service_mbus]
    @rds_mbus  = opts[:redis_mbus]
    @prov_svcs = {}
    @opts = opts
  end

  def start
    @logger.info("Starting redis service provisioner")
    @service_nats = NATS.connect(:uri => @svc_mbus) {on_service_connect}
    @redis_nats = NATS.connect(:uri => @rds_mbus) {on_node_connect}

    VCAP::Component.register(:nats => @service_nats,
                            :type => 'Redis-Service',
                            :host => @local_ip,
                            :config => @opts)
    EM.add_periodic_timer(60) {process_nodes}
    self
  end

  def shutdown
    @logger.info("Shutting down..")
    @service_nats.close
    @redis_nats.close
  end

  # Updates our internal state to match that supplied by handles
  # +handles+  An array of config handles
  def update_handles(handles)
    current   = Set.new(@prov_svcs.keys)
    supplied  = Set.new(handles.map {|h| h['service_id']})
    intersect = current & supplied

    handles_keyed = {}
    handles.each {|v| handles_keyed[v['service_id']] = v}

    to_add = supplied - intersect
    to_add.each do |h_id|
      @logger.debug("Adding handle #{h_id}")
      h = handles_keyed[h_id]
      @prov_svcs[h_id] = {
        :data        => h['configuration'],
        :credentials => h['credentials'],
        :service_d   => h_id
      }
    end

    # TODO: Handle removing existing handles if we decide to periodically sync with the CC
  end

  def process_nodes
    @nodes.delete_if {|_, timestamp| Time.now.to_i - timestamp > 300}
  end

  def on_service_connect
    @logger.debug("Connected to service mbus..")
  end

  def on_node_connect
    @logger.debug("Connected to node mbus..")
    @service_nats.subscribe("RaaS.announce") {|msg| on_node_announce(msg)}
    @service_nats.publish("RaaS.discover")
  end

  def on_node_announce(msg)
    @logger.debug("[Redis] Received Redis Node announcement: #{msg}")
    announce_message = Yajl::Parser.parse(msg)
    @nodes[announce_message["id"]] = Time.now.to_i
  end

  def unprovision_service(instance_id, &blk)
    begin
      success = true
      @logger.debug("Unprovisioning redis instance #{instance_id}")
      request = {'name' => instance_id}
      @redis_nats.publish("RaaS.unprovision", Yajl::Encoder.encode(request))
      @prov_svcs.delete(instance_id)
    rescue => e
      @logger.warn(e)
      success = nil
    end
    blk.call(success)
  end

  def provision_service(version, plan, &blk)
    @logger.debug("Attempting to provision redis instance (version=#{version}, plan=#{plan})")
    subscription = nil
    barrier = VCAP::Services::Redis::Barrier.new(:timeout => 2, :callbacks => @nodes.length) do |responses|
      @logger.debug("[Redis] Found the following Redis Nodes: #{responses.pretty_inspect}")
      @redis_nats.unsubscribe(subscription)
      unless responses.empty?
        provision_node(version, plan, responses, blk)
      end
    end
    subscription = @redis_nats.request("RaaS.discover", &barrier.callback)
  rescue => e
    @logger.warn(e)
  end

  def provision_node(version, plan, redis_nodes, blk)
    @logger.debug("Provisioning redis node (version=#{version}, plan=#{plan}, nnodes=#{redis_nodes.length})")
    node_with_most_memory = nil
    most_memory = 0

    redis_nodes.each do |redis_node_msg|
      redis_node_msg = redis_node_msg.first
      node = Yajl::Parser.parse(redis_node_msg)
      if node["available_memory"] > most_memory
        node_with_most_memory = node["id"]
        most_memory = node["available_memory"]
      end
    end

    if node_with_most_memory
      @logger.debug("Provisioning on #{node_with_most_memory}")
      request = {"plan" => plan}
      subscription = nil

      timer = EM.add_timer(2) {@redis_nats.unsubscribe(subscription)}
      subscription = @redis_nats.request("RaaS.provision.#{node_with_most_memory}",
                                        Yajl::Encoder.encode(request)) do |msg|
        EM.cancel_timer(timer)
        @redis_nats.unsubscribe(subscription)
        opts = Yajl::Parser.parse(msg)
        svc = {:data => opts, :service_id => opts['name'], :credentials => opts}
        @logger.debug("Provisioned #{svc.pretty_inspect}")
        @prov_svcs[svc[:service_id]] = svc
        blk.call(svc)
      end
    else
      @logger.warn("Could not find a redis node to provision: #{provision_request} #{reply}")
    end

  end

  def bind_instance(instance_id, binding_options, &blk)
    @logger.debug("Attempting to bind to service #{instance_id}")
    svc = @prov_svcs[instance_id]
    handle = nil
    if svc
      @logger.debug("Config: #{svc.inspect}")
      handle = {
        'service_id'    => UUIDTools::UUID.random_create.to_s,
        'configuration' => svc,
        'credentials'   => svc[:data],
      }
      @logger.debug("Binding redis instance #{instance_id} to handle #{handle['service_id']}")
    end
    blk.call(handle)
  end

  def unbind_instance(instance_id, handle_id, binding_options, &blk)
    blk.call(true)
  end

end
