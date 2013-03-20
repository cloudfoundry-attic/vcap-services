# Copyright (c) 2009-2011 VMware, Inc.
require 'time'
require 'em-http'
require 'json'
require 'json_message'
require 'services/api'
require 'fiber'
require 'uaa'

module VCAP
  module Services
    module Backup
    end
  end
end

require 'util'

module VCAP::Services::Backup::Worker

  @manager = nil
  @options = {}
  @serv_ins = {}

  include VCAP::Services::Backup::Util

  def run
    if Dir.exists?(@manager.root)
      @manager.logger.info("#{self.class}: Running");
      @serv_ins = {}
      get_live_service_ins
      scan(@manager.root)
    else
      @manager.logger.warn("Root directory for scanning is not existed.")
    end
    true
  rescue Interrupt
    raise
  rescue Exception => x
    @manager.logger.error("#{self.class}: Exception while running: #{x.message}, #{x.backtrace.join(', ')}")
    false
  end

  def handle_response_v1(http,name)
    instances = []
    if http.response_header.status == 200
      begin
        resp = VCAP::Services::Api::ListHandlesResponse.decode(http.response)
        resp.handles.each do |h|
          if h['service_id']
            service_id = h['service_id']
            #Because some old service_ids may initial with service name,
            #remove the service name for compatibility's sake.
            service_id.gsub!(/^(mongodb|redis)-/,'')
            service_id = String.new(service_id)
            (@serv_ins[name] ||= []) << service_id if service_id
          end
        end if resp.handles
      rescue => e
        @manager.logger.error("Error to parse handle: #{e.message}")
      end
    else
      @manager.logger.warn("Fetching #{name} handle ans: #{http.response_header.status}")
    end
  end

  def handle_response_v2(entity)
    begin
      if entity['credentials']['name']
        name = entity['service_plan']['entity']['service']['entity']['label']
        service_id = entity['credentials']['name']
        @manager.logger.debug("service id: #{service_id}")
        (@serv_ins[name] ||= []) << service_id if service_id
      end
    rescue => e
      @manager.logger.error("Error to parse handle: #{e.message}")
    end
  end

  def get_client_auth_token
    # Load the auth token to be sent out in Authorization header when making CCNG-v2 requests
    credentials = @options[:uaa_client_auth_credentials]
    client_id   = @options[:uaa_client_id]
    ti = CF::UAA::TokenIssuer.new(@options[:uaa_endpoint], client_id)
    token = ti.implicit_grant_with_creds(credentials).info
    uaa_client_auth_token = "#{token["token_type"]} #{token["access_token"]}"
    @manager.logger.debug("token: #{uaa_client_auth_token}")
    uaa_client_auth_token
  end

  def perform_multiple_page_get(seed_url, cc_uri)
    url = seed_url

    @manager.logger.info("Fetching from: #{cc_uri}#{url}")

    page_num = 1
    while  !url.nil? do
      rt, http = request_service_ins_fibered("#{cc_uri}#{url}")
      @manager.logger.debug("request service instance.")
      if rt == true
        if (200..299) === http.response_header.status
          result = JSON.parse(http.response)
        else
          raise "Multiple page via #{cc_uri}#{url} failed (#{http.response_header.status}) - #{http.response}"
        end

        raise "Failed parsing http response: #{http.response}" if result == nil

        result["resources"].each { |r| yield r if block_given? }

        page_num += 1

        url = result["next_url"]
        @manager.logger.debug("Fetching... pg. #{page_num} from: #{url}") unless url.nil?
      else
        @manager.logger.error("Request error from #{cc_uri}#{url}")
      end
    end
  end

  def get_service_ins_v1(cc_uri)
    @options[:services].each do |name,svc|
      version = svc['version']
      uri = "#{cc_uri}/services/v1/offerings/#{name}-#{version}/handles"
      REQ_HEADER[:head]['X-VCAP-Service-Token'] = svc['token']||'0xdeadbeef'
      result, http = request_service_ins_fibered(uri)
      if result
        handle_response_v1(http, name)
      else
        @manager.logger.error("Error at fetching handle at #{uri} ans: #{http}")
      end
      raise Interrupt, "Interrupted" if @manager.shutdown?
    end if @options[:services]
  rescue => e
    @manager.logger.error "Failed to get_live_service_ins #{e.message}"
  end

  def get_service_ins_v2(cc_uri)
    uri = "/v2/service_instances?inline-relations-depth=2"
    get_client_auth_token
    REQ_HEADER[:head]['Authorization'] = get_client_auth_token
    perform_multiple_page_get(uri, cc_uri) do |r|
      handle_response_v2(r['entity'])
    end
    raise Interrupt, "Interrupted" if @manager.shutdown?
  rescue => e
    @manager.logger.error "Failed to get_live_service_ins #{e.message}"
  end

  def get_live_service_ins
    return if @options[:cc_api_uri].nil?
    cc_uri = @options[:cc_api_uri]
    cc_uri = "http://#{cc_uri}" if !cc_uri.start_with?("http://")
    if @options[:cc_api_version] == "v1"
      get_service_ins_v1 cc_uri
    else
      get_service_ins_v2 cc_uri
    end
  end

  def prune(path, timestamp=nil )
    if timestamp
      @manager.logger.info("Pruning #{path} from #{Time.at(timestamp)}")
    else
      @manager.logger.info("Pruning #{path} ")
    end
    rmdashr(path)
    # also prune any parent directories that have become empty
    path = parent(path)
    while path != @manager.root && empty(path)
      @manager.logger.info("Pruning empty parent #{path}")
      Dir.delete(path)
      path = parent(path)
    end
  rescue => x
    @manager.logger.error("Could not prune #{path}: #{x.to_s}")
  ensure
    raise Interrupt, "Interrupted" if @manager.shutdown?
  end

end

