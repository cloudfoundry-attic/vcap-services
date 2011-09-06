#!/usr/bin/env ruby
require 'eventmachine'
require 'em-http'
require 'yajl'
require 'yaml'
require 'json'
require 'optparse'

ENV["BUNDLE_GEMFILE"] ||= File.expand_path("../../Gemfile", __FILE__)

DEFAULT_CONFIG_FILE = File.expand_path("../../config/default.yml",__FILE__)

class PurgeOrphan

  def initialize()
    @config_file = DEFAULT_CONFIG_FILE
    @config = {}
    @orphan_file = ""
    @mode = :purge_orphan
  end

  def run
    parse_options
    begin
      @config = YAML.load(File.open(@config_file))
    rescue => e
      puts "Could not read configuration file: #{e}"
      return 1
    end
    send @mode
  end

  private

  def check_orphan
    req = {
      :head => {
        "Content-Type" => "application/json",
        "X-VCAP-Service-Token" => @config["token"]
      },
      :body => {
      }.to_json
    }
    gateway_uri = @config["gateway_uri"]
    gateway_uri = "http://#{gateway_uri}" unless gateway_uri.start_with?("http://")
    uri = "#{gateway_uri}/service/internal/v1/check_orphan"
    EM.run do
      EM.add_timer(@config["timeout"].to_i) do
        puts "Timeout for the request. Exiting..."
        EM.stop
      end
      http = EM::HttpRequest.new(uri).post(req)
      http.callback do
        if http.response_header.status == 200
          puts "Successfully request to check orphan"
        else
          puts "Error in check orphan #{http.response_header.status}"
        end
        EM.stop
      end
      http.errback do
        puts "Error to check orphan #{http.error}"
        EM.stop
      end
    end
  end

  def purge_orphan
    if ARGV.empty?
      puts usage
      return 1
    end
    @orphan_file = ARGV[0]
    data = {}
    begin
      data = Yajl::Parser.new.parse(File.open(@orphan_file))
    rescue => e
      puts "Could not read orphan file: #{e}"
      return 1
    end
    req = {
      :head => {
        "Content-Type" => "application/json",
        "X-VCAP-Service-Token" => @config["token"]
      },
      :body => {
        :orphan_instances => data["orphan_instances"],
        :orphan_bindings => data["orphan_bindings"]
      }.to_json
    }
    gateway_uri = @config["gateway_uri"]
    gateway_uri = "http://#{gateway_uri}" unless gateway_uri.start_with?("http://")
    uri = "#{gateway_uri}/service/internal/v1/purge_orphan"
    EM.run do
      EM.add_timer(@config["timeout"].to_i) do
        puts "Timeout for the request. Exiting..."
        EM.stop
      end
      http = EM::HttpRequest.new(uri).delete(req)
      http.callback do
        if http.response_header.status == 200
          puts "Successfully request to purge orphan"
        else
          puts "Error in purge orphan #{http.response_header.status}"
        end
        EM.stop
      end
      http.errback do
        puts "Error in purging orphan #{http.error}"
        EM.stop
      end
    end
  end

  def parse_options
    OptionParser.new do |opts|
      opts.banner = usage
      opts.on("-c", "--config [ARG]", "Configuration File") do |opt|
        @config_file = opt
      end
      opts.on("-k","--check","Request to check orphan") do |opt|
        @mode = :check_orphan
      end
      opts.on("-h", "--help", "Help") do
        puts opts
        exit
      end
    end.parse!
  end

  def usage
    "Usage: #{File.basename($0)} [options] <orphan_file>"
  end
end

trap('TERM') {puts "\nInterupted"; exit(1)}
trap('INT') {puts "\nInterupted"; exit(1)}

PurgeOrphan.new.run
