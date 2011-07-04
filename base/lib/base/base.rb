# Copyright (c) 2009-2011 VMware, Inc.
require 'eventmachine'
require 'vcap/common'
require 'vcap/component'
require 'nats/client'

$LOAD_PATH.unshift File.dirname(__FILE__)
require 'abstract'
require 'service_error'

module VCAP
  module Services
    module Base
      class Base
      end
    end
  end
end

class Object
  def deep_dup
    Marshal::load(Marshal.dump(self))
  end
end

class VCAP::Services::Base::Base

  include VCAP::Services::Base::Error

  def initialize(options)
    @logger = options[:logger]
    @options = options
    @local_ip = VCAP.local_ip(options[:ip_route])
    @logger.info("#{service_description}: Initializing")
    @node_nats = NATS.connect(:uri => options[:mbus]) {
      on_connect_node
    }
    VCAP::Component.register(
      :nats => @node_nats,
      :type => service_description,
      :host => @local_ip,
      :index => options[:index] || 0,
      :config => options
    )
    z_interval = options[:z_interval] || 30
    EM.add_timer(5) { update_varz } # give service a chance to wake up
    EM.add_periodic_timer(z_interval) { update_varz }
    EM.add_timer(5) { update_healthz } # give service a chance to wake up
    EM.add_periodic_timer(z_interval) { update_healthz }
  end

  def service_description()
    return "#{service_name}-#{flavor}"
  end

  def update_varz()
    varz_details.each { |k,v|
      VCAP::Component.varz[k] = v
    }
  end

  def update_healthz()
    VCAP::Component.healthz = Yajl::Encoder.encode(healthz_details, :pretty => true, :terminator => "\n")
  end

  def shutdown()
    @logger.info("#{service_description}: Shutting down")
    @node_nats.close
  end

  # Subclasses VCAP::Services::Base::{Node,Provisioner} implement the
  # following methods. (Note that actual service Provisioner or Node
  # implementations should NOT need to touch these!)
  abstract :on_connect_node
  abstract :flavor # "Provisioner" or "Node"
  abstract :varz_details
  abstract :healthz_details

  # Service Provisioner and Node classes must implement the following
  # method
  abstract :service_name

end
