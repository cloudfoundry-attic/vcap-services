# Copyright (c) 2009-2011 VMware, Inc.
require "eventmachine"
require "em-http-request"
require "vcap/common"
require "vcap/component"
require "sinatra"
require "nats/client"
require "redis"
require "json"
require "sys/filesystem"
require "fileutils"
require "services/api"
require "services/api/const"

$LOAD_PATH.unshift File.dirname(__FILE__)
require "store"

include Sys

module VCAP
  module Services
    module Serialization
    end
  end
end

class VCAP::Services::Serialization::Server < Sinatra::Base

  REQ_OPTS = %w(serialization_base_dir mbus port external_uri redis).map {|o| o.to_sym}

  set :show_exceptions, false
  set :method_override, true

  def initialize(opts)
    super
    missing_opts = REQ_OPTS.select {|o| !opts.has_key? o}
    raise ArgumentError, "Missing options: #{missing_opts.join(', ')}" unless missing_opts.empty?
    @opts = opts
    @logger = opts[:logger] || make_logger
    @opts[:logger] = @logger
    @store = VCAP::Services::Serialization::Store.new(@opts)
    @nginx = opts[:nginx]
    @host = opts[:host]
    @port = opts[:port]
    @external_uri = opts[:external_uri]
    @upload_token = opts[:upload_token]
    @purge_expired_interval = opts[:purge_expired_interval] || 1200
    @router_start_channel  = nil

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

    # Setup purger for expired upload files
    EM.add_periodic_timer(@purge_expired_interval) {
      EM.defer{ @store.purge_expired }
    }

    EM.add_timer(5) {
      EM.defer{ @store.purge_expired }
    }

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

  def http_uri(uri)
    uri = "http://#{uri}" unless (uri.index('http://') == 0 || uri.index('https://') == 0)
    uri
  end

  def on_connect_nats()
    @logger.info("Register download server uri : #{@router_register_json}")
    @nats.publish('router.register', @router_register_json)
    @router_start_channel = @nats.subscribe('router.start') { @nats.publish('router.register', @router_register_json)}
    @store.connect_redis
  end

  def varz_details()
    varz = {}
    # check NFS disk free space
    free_space = 0
    begin
      stats = Filesystem.stat("#{@store.base_dir}")
      avail_blocks = stats.blocks_available
      total_blocks = stats.blocks
      free_space = format("%.2f", avail_blocks.to_f / total_blocks.to_f * 100)
    rescue => e
      @logger.error("Failed to get filesystem info of #{@store.base_dir}: #{e}")
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

  def make_logger()
    logger = Logger.new(STDOUT)
    logger.level = Logger::DEBUG
    logger
  end

  # Unregister external uri
  def send_deactivation_notice(stop_event_loop=true)
    @logger.info("Sending deactivation notice to router")
    @nats.unsubscribe(@router_start_channel) if @router_start_channel
    @logger.debug("Unregister uri: #{@router_register_json}")
    @nats.publish("router.unregister", @router_register_json)
    @nats.close
    EM.stop if stop_event_loop
  end

  def nginx_path(service, id, snapshot_id, file_name)
    File.join(@nginx["nginx_path"], "snapshots", service, id[0,2], id[2,2], id[4,2], id, snapshot_id, file_name)
  end

  def nginx_upload_file_path(service, id, token, time=nil)
    File.join(@nginx["nginx_path"], "uploads", service, id[0,2], id[2,2], id[4,2], id, (time||Time.now.to_i).to_s, token)
  end

  def generate_download_url(service, service_id, token)
    url = "http://#{@host}:#{( @nginx ? @nginx["nginx_port"] : @port)}/serialized/#{service}/#{service_id}/serialized/file?token=#{token}"
  end

  def get_uploaded_data_file
    file = nil
    if @nginx
      path = params[:data_file_path]
      wrapper_class = Class.new do
        attr_accessor :path
      end
      file = wrapper_class.new
      file.path = path
    else
      file = params[:data_file][:tempfile] if params[:data_file]
    end
    file
  end

  def authorized?
    request_header(VCAP::Services::Api::SDS_UPLOAD_TOKEN_HEADER) == @upload_token
  end

  def request_header(header)
    # This is pretty ghetto but Rack munges headers, so we need to munge them as well
    rack_hdr = "HTTP_" + header.upcase.gsub(/-/, '_')
    env[rack_hdr]
  end

  # store the uploaded file

  put "/serialized/:service/:service_id/serialized/data" do
    error(403) unless authorized?
    begin
      data_file = get_uploaded_data_file
      unless data_file && data_file.path && File.exist?(data_file.path)
        error(400)
      end
      service = params[:service]
      service_id = params[:service_id]
      @logger.debug("Upload serialized data for service=#{service}, service_id=#{service_id}")

      code, file_token, new_file_path = @store.store_file(service, service_id, data_file.path)
      if code.to_i != 200
        error(code.to_i)
      end

      # return url
      download_url = generate_download_url(service, service_id, file_token)
      status 200
      content_type :json
      resp = {:url => download_url}
      VCAP::Services::Api::SerializedURL.new(resp).encode
    rescue => e
      @logger.error("Error when store and register the uploaded file: #{e} - #{e.backtrace.join(' | ')}")
      error(400)
    ensure
      FileUtils.rm_rf(data_file.path) if data_file && data_file.path && File.exist?(data_file.path)
    end
  end

  # download uploaded data file

  get "/serialized/:service/:service_id/serialized/file" do
    # get the token of the file
    # Is it security enough?
    token = params[:token]
    error(403) unless token
    service = params[:service]
    service_id = params[:service_id]
    # if the file is expired, unregister and delete it
    file, time = @store.try_unregister_file(service, service_id, token, false)
    # send out the file if the file exists
    if file && File.exist?(file)
      if @nginx
        status 200
        content_type "application/octet-stream"
        @logger.info("Serve file using nginx: #{file}")
        @store.make_file_world_readable(file)
        response["X-Accel-Redirect"] = nginx_upload_file_path(service, service_id, token, time)
      else
        @logger.info("Serve file: #{file}")
        send_file(file)
      end
    else
      @logger.info("Can't find uploaded file for service #{service}/service_id #{service_id} with token #{token}")
      error(404)
    end
  end

  delete "/serialized/:service/:service_id/serialized/file" do
    error(403) unless authorized?
    # get the token of the file
    # Is it security enough?
    token = params[:token]
    error(403) unless token
    service = params[:service]
    service_id = params[:service_id]
    file, time = @store.try_unregister_file(service, service_id, token, true)
    unless file || time
      status 200
    else
      error(500)
    end
  end

  get "/serialized/:service/:service_id/snapshots/:snapshot_id" do
    token = params[:token]
    error(403) unless token
    service = params[:service]
    service_id = params[:service_id]
    snapshot_id = params[:snapshot_id]
    @logger.debug("Get serialized data for service=#{service}, service_id=#{service_id}, snapshot_id=#{snapshot_id}")

    code, real_path, file_name = @store.get_snapshot_file_path(service, service_id, snapshot_id, token)

    if code.to_i != 200
      error(code.to_i)
    end

    if (File.exists? real_path)
      if @nginx
        status 200
        content_type "application/octet-stream"
        path = nginx_path(service, service_id, snapshot_id, file_name)
        @logger.info("Serve file using nginx: #{real_path}")
        @store.make_file_world_readable(real_path)
        response["X-Accel-Redirect"] = path
      else
        @logger.info("Serve file: #{real_path}")
        send_file(real_path)
      end
    else
      @logger.info("Can't find file:#{real_path}")
      error(404)
    end
  end

  not_found do
    halt 404
  end

end
