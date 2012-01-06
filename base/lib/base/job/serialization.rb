# Copyright (c) 2009-2011 VMware, Inc.
require "resque/job_with_status"
require "fileutils"
require "curb"
require "vcap/logging"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..')
require "base/service_error"

module VCAP
  module Services
  end
end

module VCAP::Services::Serialization
  SERIALIZATION_KEY_PREFIX = "vcap:serialization".freeze

  class << self
    attr_reader :redis, :logger

    # Config the redis using options
    def redis_connect(opts)
      resque = %w(host port password).inject({}){|res, o| res[o.to_sym] = opts[o]; res}
      @redis = ::Redis.new(resque)
    end

    def redis=(redis)
      raise "Serialization requires redis configuration." unless redis
      @redis = redis
    end

    def logger=(logger)
      @logger = logger
    end
  end

  def redis_key(key)
    "#{SERIALIZATION_KEY_PREFIX}:#{key}"
  end

  class SerializationJob < Resque::JobWithStatus
    include VCAP::Services::Serialization
    include VCAP::Services::Base::Error

    class << self
      attr_reader :logger

      def queue_lookup_key
        :node_id
      end

      def logger=(logger)
        @logger = logger
      end

      def select_queue(*args)
        result = nil
        args.each do |arg|
          result = arg[queue_lookup_key]if (arg.is_a? Hash )&& (arg.has_key?(queue_lookup_key))
        end
        @logger.info("Select queue #{result} for job #{self.class} with args:#{args.inspect}") if @logger
        result
      end
    end

    def initialize(*args)
      super(*args)
      parse_config
      init_worker_logger()
    end

    def client
      VCAP::Services::Serialization.redis
    end

    def init_worker_logger
      VCAP::Logging.setup_from_config(@config["logging"])
      @logger = VCAP::Logging.logger("#{@config["service_name"]}_worker")
    end

    # the serialize path structure looks like <base-dir>\serialize\<service-name>\<aa>\<bb>\<cc>\
    # <aabbcc-rest-of-instance-guid>\<serialization data>
    def get_serialized_data_path(name)
      File.join(@config["serialization_base_dir"], "serialize", @config["service_name"] , name[0,2],name[2,2], name[4,2], name)
    end

    # Update the download token for a serialized file and save it in redis
    def update_download_token(service, name, file_name, token)
      key = "#{service}:#{name}:token"
      client.hset(redis_key(key), :token, token)
      client.hset(redis_key(key), :file, file_name)
    end

    def parse_config
      @config = Yajl::Parser.parse(ENV['WORKER_CONFIG'])
      raise "Need environment variable: WORKER_CONFIG" unless @config
    end

    def cleanup(name)
      return unless name
      FileUtils.rm_rf(get_serialized_data_path(name))
    end

    # Fetch remote uri and stream content to file.
    def fetch_url(url, file_path)
      # TODO check the file size before download?
      File.open(file_path, "wb+") do |f|
        c = Curl::Easy.new(url)
        c.on_body{|data| f.write(data)}
        c.perform
      end
    end
  end
end
