# Copyright (c) 2009-2011 VMware, Inc.
require 'rubygems'
require 'bundler/setup'

require 'optparse'
require 'logger'
require 'logging'
require 'net/http'
require 'thin'
require 'yaml'

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..')
require 'vcap/common'
require 'vcap/logging'

$LOAD_PATH.unshift File.dirname(__FILE__)
require 'asynchronous_service_gateway'
require 'abstract'

module VCAP
  module Services
    module Base
    end
  end
end


class VCAP::Services::Base::Gateway

  abstract :default_config_file
  abstract :provisioner_class

  CC_CONFIG_FILE = File.expand_path("../../../../../cloud_controller/config/cloud_controller.yml", __FILE__)

  def start

    config_file = default_config_file

    OptionParser.new do |opts|
      opts.banner = "Usage: $0 [options]"
      opts.on("-c", "--config [ARG]", "Configuration File") do |opt|
        config_file = opt
      end
      opts.on("-h", "--help", "Help") do
        puts opts
        exit
      end
    end.parse!

    begin
      config = parse_gateway_config(config_file)
    rescue => e
      puts "Couldn't read config file: #{e}"
      exit
    end

    VCAP::Logging.setup_from_config(config[:logging])
    # Use the current running binary name for logger identity name, since service gateway only has one instance now.
    logger = VCAP::Logging.logger(File.basename($0))
    config[:logger] = logger

    if config[:pid]
      pf = VCAP::PidFile.new(config[:pid])
      pf.unlink_at_exit
    end

    config[:host] = VCAP.local_ip(config[:ip_route])
    config[:port] ||= VCAP.grab_ephemeral_port
    config[:service][:label] = "#{config[:service][:name]}-#{config[:service][:version]}"
    config[:service][:url]   = "http://#{config[:host]}:#{config[:port]}"
    cloud_controller_uri = config[:cloud_controller_uri] || default_cloud_controller_uri

    params = {
      :logger   => logger,
      :version  => config[:service][:version],
      :local_ip => config[:host],
      :mbus => config[:mbus],
      :node_timeout => config[:node_timeout] || 2
    }
    if config.has_key?(:aux)
      params[:aux] = config[:aux]
    end

    # Go!
    EM.run do
      sp = provisioner_class.new(
             :logger   => logger,
             :index    => config[:index],
             :version  => config[:service][:version],
             :ip_route => config[:ip_route],
             :mbus => config[:mbus],
             :node_timeout => config[:node_timeout] || 2,
             :allow_over_provisioning => config[:allow_over_provisioning]
           )
      sg = VCAP::Services::AsynchronousServiceGateway.new(
             :proxy => config[:proxy],
             :service => config[:service],
             :token   => config[:token],
             :logger  => logger,
             :provisioner => sp,
             :cloud_controller_uri => cloud_controller_uri
           )
      Thin::Server.start(config[:host], config[:port], sg)
    end
  end

  def default_cloud_controller_uri
    config = YAML.load_file(CC_CONFIG_FILE)
    config['external_uri'] || "api.vcap.me"
  end

  def parse_gateway_config(config_file)
    config = YAML.load_file(config_file)
    config = VCAP.symbolize_keys(config)

    token = config[:token]
    raise "Token missing" unless token
    raise "Token must be a String or Int, #{token.class} given" unless (token.is_a?(Integer) || token.is_a?(String))
    config[:token] = token.to_s

    config
  end
end
