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

class VCAP::Services::Base::Base

  include VCAP::Services::Base::Error

  def initialize(options)
    @logger = options[:logger]
    @local_ip = VCAP.local_ip(options[:ip_route])
    @logger.info("#{service_description}: Initializing")
    @node_nats = NATS.connect(:uri => options[:mbus]) {
      on_connect_node
    }
    VCAP::Component.register(
      :nats => @node_nats,
      :type => service_description,
      :host => @local_ip,
      :config => options
    )
  end

  def service_description()
    return "#{service_name}-#{flavor}"
  end

  abstract :service_name

  abstract :on_connect_node

  abstract :flavor # "Provisioner" or "Node"

  def shutdown()
    @logger.info("#{service_description}: Shutting down")
    @node_nats.close
  end

end
