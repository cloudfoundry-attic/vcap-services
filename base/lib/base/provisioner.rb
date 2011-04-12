# Copyright (c) 2009-2011 VMware, Inc.
require "pp"
require "set"
require "datamapper"
require "uuidtools"

$LOAD_PATH.unshift File.dirname(__FILE__)
require 'base/base'
require 'barrier'

class VCAP::Services::Base::Provisioner < VCAP::Services::Base::Base

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
    current   = Set.new(@prov_svcs.keys)
    supplied  = Set.new(handles.map {|h| h['service_id']})
    intersect = current & supplied

    handles_keyed = {}
    handles.each {|v| handles_keyed[v['service_id']] = v}

    to_add = supplied - intersect
    to_add.each do |h_id|
      @logger.debug("[#{service_description}] Adding handle #{h_id}")
      h = handles_keyed[h_id]
      @prov_svcs[h_id] = {
        :data        => h['configuration'],
        :credentials => h['credentials'],
        :service_id   => h_id
      }
    end

    @logger.debug("handles updated prov_svcs: #{@prov_svcs}")
    # TODO: Handle removing existing handles if we decide to periodically sync with the CC
  end

  def find_all_bindings(name)
    res = []
    @prov_svcs.each do |k,v|
      # FIXME workaround for handles with 1 outer format, 2 inner format.
      configuration = (v[:configuration].nil?) ? v[:data] : v[:configuration]
      id = (configuration.nil?) ? nil : configuration['name']
      res << v[:credentials] if id == name && v[:service_id] != name
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
    @node_nats.publish("#{service_name}.discover")
  end

  def on_node_announce(msg)
    @logger.debug("[#{service_description}] Received node announcement: #{msg}")
    announce_message = Yajl::Parser.parse(msg)
    @nodes[announce_message["id"]] = Time.now.to_i if announce_message["id"]
  end

  def unprovision_service(instance_id, &blk)
    begin
      success = true
      svc = @prov_svcs[instance_id]
      node_id = svc[:data]["node_id"]
      bindings = find_all_bindings(instance_id)
      @logger.debug("[#{service_description}] Unprovisioning instance #{instance_id} from #{node_id}")
      request = {
        'name'     => instance_id,
        'bindings' => bindings
      }
      @logger.debug("[#{service_description}] Sending reqeust #{request}")
      @node_nats.publish("#{service_name}.unprovision.#{node_id}", Yajl::Encoder.encode(request))

      @prov_svcs.delete(instance_id)
      bindings.each do |b|
        @prov_svcs.delete(b[:service_id])
      end
    rescue => e
      @logger.warn(e)
      success = nil
    end
    blk.call(success)
  end

  def provision_service(version, plan, &blk)
    @logger.debug("[#{service_description}] Attempting to provision instance (version=#{version}, plan=#{plan})")
    subscription = nil
    barrier = VCAP::Services::Base::Barrier.new(:timeout => @node_timeout, :callbacks => @nodes.length) do |responses|
      @logger.debug("[#{service_description}] Found the following nodes: #{responses.pretty_inspect}")
      @node_nats.unsubscribe(subscription)
      unless responses.empty?
        provision_node(version, plan, responses, blk)
      end
    end
    subscription = @node_nats.request("#{service_name}.discover", &barrier.callback)
  rescue => e
    @logger.warn(e)
  end

  def provision_node(version, plan, node_msgs, blk)
    @logger.debug("[#{service_description}] Provisioning node (version=#{version}, plan=#{plan}, nnodes=#{node_msgs.length})")
    nodes = node_msgs.map { |msg| Yajl::Parser.parse(msg.first) }
    best_node = nodes.max_by { |node| node_score(node) }
    if best_node && node_score(best_node) > 0
      best_node = best_node["id"]
      @logger.debug("[#{service_description}] Provisioning on #{best_node}")
      request = {"plan" => plan}
      subscription = nil
      timer = EM.add_timer(@node_timeout) {@node_nats.unsubscribe(subscription)}
      subscription =
        @node_nats.request(
          "#{service_name}.provision.#{best_node}",
          Yajl::Encoder.encode(request)
       ) do |msg|
          EM.cancel_timer(timer)
          @node_nats.unsubscribe(subscription)
          opts = Yajl::Parser.parse(msg)
          svc = {:data => opts, :service_id => opts['name'], :credentials => opts}
          @logger.debug("Provisioned #{svc.pretty_inspect}")
          @prov_svcs[svc[:service_id]] = svc
          blk.call(svc)
        end
    else
      @logger.warn("#{service_description}: Could not find a node to provision")
    end
  end

  def bind_instance(instance_id, binding_options, &blk)
    @logger.debug("Attempting to bind to service #{instance_id}")

    if instance_id.nil?
      @logger.warn("#{instance_id} not found!")
    end

    begin
      svc = @prov_svcs[instance_id]
      raise "#{instance_id} not found!" if svc.nil?

      @logger.debug("svc[data]: #{svc[:data]}")
      node_id = svc[:data]["node_id"]
      raise "node_id not found for #{instance_id}!" if node_id.nil?

      @logger.debug("[#{service_description}] bind instance #{instance_id} from #{node_id}")
      #FIXME options = {} currently, should parse it in future.
      request = {
        'name'      => instance_id,
        'bind_opts' => binding_options
      }
      subscription = nil
      timer = EM.add_timer(2) {@node_nats.unsubscribe(subscription)}
      subscription =
        @node_nats.request( "#{service_name}.bind.#{node_id}",
          Yajl::Encoder.encode(request)
       ) do |msg|
          EM.cancel_timer(timer)
          @node_nats.unsubscribe(subscription)
          opts = Yajl::Parser.parse(msg)
          res = {
            :service_id => UUIDTools::UUID.random_create.to_s,
            :configuration => svc[:data],
            :credentials => opts
          }

          @logger.debug("Binded: #{res.pretty_inspect}")
          @prov_svcs[res[:service_id]] = res
          blk.call(res)
        end
    rescue => e
      @logger.warn(e)
      blk.call(nil)
    end
  end

  def unbind_instance(instance_id, handle_id, binding_options, &blk)
    begin
      success = true
      raise "instance_id cannot be nil" if instance_id.nil?

      svc = @prov_svcs[handle_id]
      raise "#{handle_id} not found!" if svc.nil?

      # FIXME workaround for handles with 1 outer format, 2 inner format.
      configuration = (svc[:configuration].nil?) ? svc[:data] : svc[:configuration]
      @logger.debug("svc[configuration]: #{configuration}")
      node_id = configuration["node_id"]
      raise "node_id not found for #{handle_id}!" if node_id.nil?

      @logger.debug("[#{service_description}] Unbind instance #{handle_id} from #{node_id}")
      request = svc[:credentials]
      @node_nats.publish("#{service_name}.unbind.#{node_id}", Yajl::Encoder.encode(request))
      @prov_svcs.delete(handle_id)
    rescue => e
      @logger.warn(e)
      success = nil
    end
    blk.call(success)
  end

  # subclasses must implement the following methods

  # node_score(node) -> number.  provisioners are expected to
  # provision on the "best" node (lowest load, most free capacity,
  # etc). this method should return a number; higher scores represent
  # "better" nodes; negative/zero scores mean that a node should be
  # ignored
  abstract :node_score

end
