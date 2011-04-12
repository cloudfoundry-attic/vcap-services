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

require "mysql_service/barrier"

module VCAP
  module Services
    module Mysql
    end
  end
end

class VCAP::Services::Mysql::Provisioner

  def initialize(opts)
    @logger    = opts[:logger]
    @version   = opts[:version]
    @local_ip  = VCAP.local_ip(opts[:local_ip])
    @svc_mbus  = opts[:service_mbus]
    @mysql_mbus  = opts[:mysql_mbus]
    @node_timeout = opts[:node_timeout]
    @prov_svcs = {}
    @nodes     = {}
    @opts = opts
  end

  def start
    @logger.info("Starting Mysql-Service Provisioner..")
    @service_nats = NATS.connect(:uri => @svc_mbus) {on_service_connect}
    @mysql_nats = NATS.connect(:uri => @mysql_mbus) {on_node_connect}
    VCAP::Component.register(:nats => @service_nats,
                            :type => 'Mysql-Service',
                            :host => @local_ip,
                            :config => @opts)
    EM.add_periodic_timer(60) {process_nodes}
    self
  end

  def shutdown
    @logger.info("Shutting down..")
    @service_nats.close
    @mysql_nats.close
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
    @service_nats.subscribe("MyaaS.announce") {|msg| on_node_announce(msg)}
    @service_nats.publish("MyaaS.discover")
  end

  def on_node_announce(msg)
    @logger.debug("[Mysql] Received Mysql Node announcement: #{msg}")
    announce_message = Yajl::Parser.parse(msg)
    @nodes[announce_message["id"]] = Time.now.to_i
  end

  def unprovision_service(instance_id, &blk)
    begin
      success = true
      @logger.debug("Unprovisioning Mysql instance #{instance_id}")
      request = {'name' => instance_id}
      @mysql_nats.publish("MyaaS.unprovision", Yajl::Encoder.encode(request))
      @prov_svcs.delete(instance_id)
    rescue => e
      @logger.warn(e)
      success = nil
    end
    blk.call(success)
  end

  def provision_service(version, plan, &blk)
    @logger.debug("Attempting to provision MySQL instance (version=#{version}, plan=#{plan})")
    subscription = nil
    barrier = VCAP::Services::Mysql::Barrier.new(:timeout => @node_timeout, :callbacks => @nodes.length) do |responses|
      @logger.debug("[Mysql] Found the following Mysql Nodes: #{responses.pretty_inspect}")
      @mysql_nats.unsubscribe(subscription)
      unless responses.empty?
        provision_node(version, plan, responses, blk)
      end
    end
    subscription = @mysql_nats.request("MyaaS.discover", &barrier.callback)
  rescue => e
    @logger.warn(e)
  end

  def provision_node(version, plan, mysql_nodes, blk)
    @logger.debug("Provisioning MySQL node (version=#{version}, plan=#{plan}, nnodes=#{mysql_nodes.length})")
    node_with_most_storage = nil
    most_storage = 0

    mysql_nodes.each do |mysql_node_msg|
      mysql_node_msg = mysql_node_msg.first
      node = Yajl::Parser.parse(mysql_node_msg)
      if node["available_storage"] > most_storage
        node_with_most_storage = node["id"]
        most_storage = node["available_storage"]
      end
    end

    if node_with_most_storage
      @logger.debug("Provisioning on #{node_with_most_storage}")
      request = {"plan" => plan}
      subscription = nil

      timer = EM.add_timer(@node_timeout) do
        @logger.debug("Timed out attempting to provision database on #{node_with_most_storage}")
        @mysql_nats.unsubscribe(subscription)
      end
      subscription = @mysql_nats.request("MyaaS.provision.#{node_with_most_storage}",
                                        Yajl::Encoder.encode(request)) do |msg|
        EM.cancel_timer(timer)
        @mysql_nats.unsubscribe(subscription)
        opts = Yajl::Parser.parse(msg)
        svc = {:data => opts, :service_id => opts['name'], :credentials => opts}
        @logger.debug("Provisioned #{svc.pretty_inspect} on #{node_with_most_storage}")
        @prov_svcs[svc[:service_id]] = svc
        blk.call(svc)
      end
    else
      @logger.warn("Could not find a mysql node to provision: (version=#{version}, plan=#{plan}, nnodes=#{mysql_nodes.length})")
    end

  end

  def bind_instance(instance_id, binding_options, &blk)
    svc = @prov_svcs[instance_id]
    handle = nil
    if svc
      handle = {
        :service_id => UUIDTools::UUID.random_create.to_s,
        :configuration => svc,
        :credentials   => svc[:data],
      }
      @logger.debug("Binding MySQL instance #{instance_id} to handle #{handle[:service_id]}")
    end
    blk.call(handle)
  end

  def unbind_instance(instance_id, handle_id, binding_options,&blk)
    blk.call(true)
  end

end
