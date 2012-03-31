# Copyright (c) 2009-2011 VMware, Inc.
require "eventmachine"
require "vcap/common"
require "vcap/component"
require "sinatra"
require "nats/client"
require "redis"
require "json"
require "sys/filesystem"
include Sys

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', 'mysql')

module VCAP
  module Services
    module Serialization
    end
  end
end

class VCAP::Services::Serialization::Server < Sinatra::Base

  REQ_OPTS = %w(serialization_base_dir mbus port external_uri redis).map {|o| o.to_sym}

  set :show_exceptions, false

  def initialize(opts)
    super
    missing_opts = REQ_OPTS.select {|o| !opts.has_key? o}
    raise ArgumentError, "Missing options: #{missing_opts.join(', ')}" unless missing_opts.empty?
    @opts = opts
    @logger = opts[:logger] || make_logger
    @nginx = opts[:nginx]
    @host = opts[:host]
    @port = opts[:port]
    @external_uri = opts[:external_uri]
    @router_start_channel  = nil
    @base_dir = opts[:serialization_base_dir]
    NATS.on_error do |e|
      if e.kind_of? NATS::ConnectError
        @logger.error("EXITING! NATS connection failed: #{e}")
        exit
      else
        @logger.error("NATS problem, #{e}")
      end
    end
    @nats = NATS.connect(:uri => opts[:mbus]) {
      VCAP::Component.register(
        :nats => @nats,
        :type => "SerializationDataServer",
        :index => opts[:index] || 0,
        :config => opts
      )

      on_connect_nats
    }

    z_interval = opts[:z_interval] || 30
    EM.add_periodic_timer(z_interval) do
      EM.defer { update_varz }
    end if @nats

    # Defer 5 seconds to give service a change to wake up
    EM.add_timer(5) do
      EM.defer { update_varz }
    end if @nats

    Kernel.at_exit do
      if EM.reactor_running?
        send_deactivation_notice(false)
      else
        EM.run { send_deactivation_notice }
      end
    end

    @router_register_json  = {
      :host => @host,
      :port => ( @nginx ? @nginx["nginx_port"] : @port),
      :uris => [ @external_uri ],
      :tags => {:components =>  "SerializationDataServer"},
    }.to_json
  end

  def on_connect_nats()
    @logger.info("Register download server uri : #{@router_register_json}")
    @nats.publish('router.register', @router_register_json)
    @router_start_channel = @nats.subscribe('router.start') { @nats.publish('router.register', @router_register_json)}
    @redis = connect_redis
  end

  def varz_details()
    varz = {}
    # check NFS disk free space
    free_space = 0
    begin
      stats = Filesystem.stat("#{@base_dir}")
      avail_blocks = stats.blocks_available
      total_blocks = stats.blocks
      free_space = format("%.2f", avail_blocks.to_f / total_blocks.to_f * 100)
    rescue => e
      @logger.error("Failed to get filesystem info of #{@base_dir}: #{e}")
    end
    varz[:nfs_free_space] = free_space

    varz
  end

  def update_varz()
    varz = varz_details
    varz.each { |k, v|
      VCAP::Component.varz[k] = v
    }
  end

  def connect_redis()
    redis_config = %w(host port password).inject({}){|res, o| res[o.to_sym] = @opts[:redis][o]; res}
    Redis.new(redis_config)
  end

  def make_logger()
    logger = Logger.new(STDOUT)
    logger.level = Logger::DEBUG
    logger
  end

  # Unrigister external uri
  def send_deactivation_notice(stop_event_loop=true)
    @nats.unsubscribe(@router_start_channel) if @router_start_channel
    @logger.debug("Unregister uri: #{@router_register_json}")
    @nats.publish("router.unregister", @router_register_json)
    @nats.close
    EM.stop if stop_event_loop
  end

  def redis_key(service, service_id)
    "vcap:serialization:#{service}:#{service_id}:token"
  end

  def file_path(service, id, file_name)
    File.join(@base_dir, "serialize", service, id[0,2], id[2,2], id[4,2], id, file_name)
  end

  def nginx_path(service, id)
    File.join(@nginx["nginx_path"], "serialize", service, id[0,2], id[2,2], id[4,2], id, "#{id}.gz")
  end

  get "/serialized/:service/:service_id" do
    token = params[:token]
    error(403) unless token
    service = params[:service]
    service_id = params[:service_id]
    @logger.debug("Get serialized data for service=#{service}, service_id=#{service_id}")
    key = redis_key(service, service_id)
    result = @redis.hget(key, :token)
    if not result
      @logger.info("Can't find token for service=#{service}, service_id=#{service_id}")
      error(404)
    end
    error(403) unless token == result
    file_name = @redis.hget(key, :file)
    if not file_name
      @logger.error("Can't get serialized filename from redis using key:#{key}.")
      error(501)
    end
    path = file_path(service, service_id, file_name)
    if (File.exists? path)
      if @nginx
        status 200
        content_type "application/octet-stream"
        path = nginx_path(service, service_id)
        @logger.info("Serve file using nginx: #{path}")
        response["X-Accel-Redirect"] = path
      else
        @logger.info("Serve file: #{path}")
        send_file(path)
      end
    else
      @logger.info("Can't find file:#{path}")
      error(404)
    end
  end

  not_found do
    halt 404
  end

end
