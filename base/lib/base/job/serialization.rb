# Copyright (c) 2009-2011 VMware, Inc.
require "resque-status"
require "fileutils"
require "curb"
require "vcap/logging"
# explictly import uuid to resolve namespace conflict between uuid and uuidtools gems.
require "uuid"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..')
require "base/service_error"

module VCAP::Services::Base::AsyncJob
  module Serialization
    SERIALIZATION_KEY_PREFIX = "vcap:serialization".freeze

    class << self
      attr_reader :redis

      def redis_connect
        @redis = ::Redis.new(Config.redis_config)
      end
    end

    def redis_key(key)
      "#{SERIALIZATION_KEY_PREFIX}:#{key}"
    end

    def fmt_error(e)
      "#{e}: [#{e.backtrace.join(" | ")}]"
    end

    class SerializationJob
      attr_reader :name
      include Serialization
      include Resque::Plugins::Status
      include VCAP::Services::Base::Error

        class << self

          def queue_lookup_key
            :node_id
          end

          def select_queue(*args)
            result = nil
            args.each do |arg|
              result = arg[queue_lookup_key]if (arg.is_a? Hash )&& (arg.has_key?(queue_lookup_key))
            end
            @logger = Config.logger
            @logger.info("Select queue #{result} for job #{self.class} with args:#{args.inspect}") if @logger
            result
          end
        end

      def initialize(*args)
        super(*args)
        parse_config
        init_worker_logger
        Serialization.redis_connect
      end

      def create_lock
        lock_name = "lock:lifecycle:#{name}"
        lock = Lock.new(lock_name, :logger => @logger)
        lock
      end

      def client
        Serialization.redis
      end

      def init_worker_logger
        @logger = Config.logger
      end

      def handle_error(e)
        @logger.error("Error in #{self.class} uuid:#{@uuid}: #{fmt_error(e)}")
        err = (e.instance_of?(ServiceError)? e : ServiceError.new(ServiceError::INTERNAL_ERROR)).to_hash
        err_msg = Yajl::Encoder.encode(err)
        failed(err_msg)
      end

      def required_options(*args)
        missing_opts = args.select{|arg| !options.has_key? arg.to_s}
        raise ArgumentError, "Missing #{missing_opts.join(', ')} in options: #{options.inspect}" unless missing_opts.empty?
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

      # Validate the serialized data file.
      # Sub class should override this method to supply specific validation.
      def validate_input(file_path)
        File.open(file_path) do |f|
          return nil unless f.size > 0
        end
        true
      end

    end

    class BaseCreateSerializedURLJob < SerializationJob
      attr_reader :dump_path

      VALID_CREDENTIAL_CHARACTERS = ("A".."Z").to_a + ("a".."z").to_a + ("0".."9").to_a

      # workflow template
      # Sub class should return a hash contains filename that generated on shared storage. For example
      # {:dump_file_name => 'db1.tgz'}
      def perform
        begin
          required_options :service_id
          @name = options["service_id"]
          @logger.info("Launch job: #{self.class} for #{name}")

          lock = create_lock
          lock.lock do
            result = execute
            @logger.info("Results of create serialized url: #{result}")

            token = generate_download_token()
            service_name = @config["service_name"]
            update_download_token(service_name, name, result[:dump_file_name], token)
            url = generate_download_url(name, token)
            @logger.info("Download link generated for #{name}: #{url}")

            job_result = { :url => url }
            completed(Yajl::Encoder.encode(job_result))
            @logger.info("Complete job: #{self.class} for #{name}")
          end
        rescue => e
          cleanup(name)
          handle_error(e)
        ensure
          set_status({:complete_time => Time.now.to_s})
        end
      end

      def generate_download_token(length=12)
        Array.new(length) { VALID_CREDENTIAL_CHARACTERS[rand(VALID_CREDENTIAL_CHARACTERS.length)] }.join
      end

      def generate_download_url(name, token)
        service = @config["service_name"]
        url_template = @config["download_url_template"]
        eval "\"#{url_template}\""
      end
    end

    class BaseImportFromURLJob < SerializationJob
      attr_reader :url, :temp_file_path
      # Sub class should return true for a successful import job.
      def perform
        begin
          required_options :service_id, :url
          @name = options["service_id"]
          @url = options["url"]
          @logger.info("Launch job: #{self.class} for #{name}")

          lock = create_lock
          lock.lock do
            @temp_file_path = File.join(@config["tmp_dir"], "#{name}")
            FileUtils.rm_rf(temp_file_path)
            fetch_url(url, temp_file_path)
            raise ServiceError.new(ServiceError::BAD_SERIALIZED_DATAFILE, url) unless validate_input(temp_file_path)
            result = execute
            @logger.info("Results of import from url: #{result}")

            job_result = { :result => :ok }
            completed(Yajl::Encoder.encode(job_result))
            @logger.info("Complete job: #{self.class} for #{name}")
          end
        rescue => e
          handle_error(e)
        ensure
          set_status({:complete_time => Time.now.to_s})
          FileUtils.rm_rf(temp_file_path) if temp_file_path
        end
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

    class BaseImportFromDataJob < SerializationJob
      attr_reader :temp_file_path
      # Sub class should return true for a successful import job.
      def perform
        begin
          required_options :service_id, :temp_file_path
          @name = options["service_id"]
          @temp_file_path = options["temp_file_path"]
          @logger.info("Launch job: #{self.class} for #{name}")

          lock = create_lock
          lock.lock do
            raise "Can't find temp file: #{@temp_file_path}" unless File.exists? temp_file_path
            raise ServiceError.new(SerivceError::BAD_SERIALIZED_DATAFILE, "request") unless validate_input(temp_file_path)

            result = execute
            @logger.info("Results of import from url: #{result}")

            job_result = { :result => :ok }
            completed(Yajl::Encoder.encode(job_result))
            @logger.info("Complete job: #{self.class} for #{name}")
          end
        rescue => e
          handle_error(e)
        ensure
          set_status({:complete_time => Time.now.to_s})
          FileUtils.rm_rf(@temp_file_path) if @temp_file_path
        end
      end
    end
  end
end
