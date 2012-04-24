# Copyright (c) 2009-2012 VMware, Inc.
require "rubygems"
require 'bundler/setup'
require 'optparse'
require 'yaml'
require 'resque'

require "vcap/common"
require "vcap/logging"

$LOAD_PATH.unshift File.dirname(__FILE__)
require "abstract"
require "job/config"

module VCAP
  module Services
    module Base
    end
  end
end

module VCAP::Services::Base
  class WorkerBin
    abstract :default_config_file

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

      required_opts  = %w(resque)
      missing_opts = required_opts.select {|o| !config.has_key? o}
      raise ArgumentError, "Missing options: #{missing_opts.join(', ')}" unless missing_opts.empty?

      VCAP::Logging.setup_from_config(config["logging"])

      redis_config = config["resque"]
      logger = VCAP::Logging.logger(config["node_id"])
      redis_config = %w(host port password).inject({}){|res, o| res[o.to_sym] = config["resque"][o]; res}
      AsyncJob::Config.redis_config = redis_config
      AsyncJob::Config.logger = logger

      ENV['WORKER_CONFIG'] = Yajl::Encoder.encode(config)

      # Use node_id as default queue name if no queues configuration is given
      queues = (config["queues"] || config["node_id"]).split(',')

      worker = Resque::Worker.new(*queues)
      worker.verbose = config["resque_worker_logging"]

      pid_file = ENV['PIDFILE']
      raise "worker need PIDFILE env var." unless pid_file
      File.open(pid_file, "w") {|f| f << worker.pid}

      logger.info("Starting worker: #{worker}")
      worker.work(config["interval"]||5)
    end
  end
end
