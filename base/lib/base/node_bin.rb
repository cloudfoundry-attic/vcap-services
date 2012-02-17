# Copyright (c) 2009-2011 VMware, Inc.
require 'rubygems'
require 'bundler/setup'
require 'optparse'
require 'logger'
require 'logging'
require 'yaml'

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..')
require 'vcap/common'
require 'vcap/logging'

$LOAD_PATH.unshift File.dirname(__FILE__)
require 'abstract'

module VCAP
  module Services
    module Base
    end
  end
end


class VCAP::Services::Base::NodeBin

  abstract :default_config_file
  abstract :node_class
  abstract :additional_config

  def start
    config_file = default_config_file

    OptionParser.new do |opts|
      opts.banner = "Usage: #{$0.split(/\//)[-1]} [options]"
      opts.on("-c", "--config [ARG]", "Configuration File") do |opt|
        config_file = opt
      end
      opts.on("-h", "--help", "Help") do
        puts opts
        exit
      end
    end.parse!

    begin
      config = YAML.load_file(config_file)
    rescue => e
      puts "Could not read configuration file:  #{e}"
      exit
    end

    options = {
      :index => parse_property(config, "index", Integer, :optional => true),
      :plan => parse_property(config, "plan", String, :optional => true, :default => "free"),
      :capacity => parse_property(config, "capacity", Integer, :optional => true, :default => 200),
      :base_dir => parse_property(config, "base_dir", String),
      :ip_route => parse_property(config, "ip_route", String, :optional => true),
      :node_id => parse_property(config, "node_id", String),
      :z_interval => parse_property(config, "z_interval", Integer, :optional => true),
      :mbus => parse_property(config, "mbus", String),
      :local_db => parse_property(config, "local_db", String),
      :migration_nfs => parse_property(config, "migration_nfs", String, :optional => true),
      :max_nats_payload => parse_property(config, "max_nats_payload", Integer, :optional => true)
    }

    VCAP::Logging.setup_from_config(config["logging"])
    # Use the node id for logger identity name.
    options[:logger] = VCAP::Logging.logger(options[:node_id])
    @logger = options[:logger]

    options = additional_config(options, config)

    EM.error_handler do |e|
      @logger.fatal("#{e} #{e.backtrace.join("|")}")
      exit
    end

    pid_file = parse_property(config, "pid", String)
    begin
      FileUtils.mkdir_p(File.dirname(pid_file))
    rescue => e
      @logger.fatal "Can't create pid directory, exiting: #{e}"
      exit
    end
    File.open(pid_file, 'w') { |f| f.puts "#{Process.pid}" }

    EM.run do
      node = node_class.new(options)
      trap("INT") {shutdown(node)}
      trap("TERM") {shutdown(node)}
    end
  end

  def shutdown(node)
    @logger.info("Begin to shutdown node")
    node.shutdown
    @logger.info("End to shutdown node")
    EM.stop
  end

  def parse_property(hash, key, type, options = {})
    obj = hash[key]
    if obj.nil?
      raise "Missing required option: #{key}" unless options[:optional]
      options[:default]
    elsif type == Range
      raise "Invalid Range object: #{obj}" unless obj.kind_of?(Hash)
      first, last = obj["first"], obj["last"]
      raise "Invalid Range object: #{obj}" unless first.kind_of?(Integer) and last.kind_of?(Integer)
      Range.new(first, last)
    else
      raise "Invalid #{type} object: #{obj}" unless obj.kind_of?(type)
      obj
    end
  end
end
