# Copyright (c) 2009-2011 VMware, Inc.
require "resque-status"
require "fileutils"
require "curb"
require "vcap/logging"
# explictly import uuid to resolve namespace conflict between uuid and uuidtools gems.
require "uuid"

$LOAD_PATH.unshift File.dirname(__FILE__)
require "snapshot"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..')
require "service_error"

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
      include Snapshot
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
        Snapshot.redis_connect
      end

      def create_lock
        lock_name = "lock:lifecycle:#{name}"
        ttl = @config[:job_ttl] || 600
        lock = Lock.new(lock_name, :logger => @logger, :ttl => ttl)
        lock
      end

      def init_worker_logger
        @logger = Config.logger
      end

      def handle_error(e)
        @logger.error("Error in #{self.class} uuid:#{@uuid}: #{fmt_error(e)}")
        err = (e.instance_of?(ServiceError)? e : ServiceError.new(ServiceError::INTERNAL_ERROR)).to_hash
        err_msg = Yajl::Encoder.encode(err["msg"])
        failed(err_msg)
      end

      def required_options(*args)
        missing_opts = args.select{|arg| !options.has_key? arg.to_s}
        raise ArgumentError, "Missing #{missing_opts.join(', ')} in options: #{options.inspect}" unless missing_opts.empty?
      end

      # Update the download token for a service snapshot
      def update_download_token(name, snapshot_id, token)
        snapshot = snapshot_details(name, snapshot_id)
        snapshot["token"] = token
        save_snapshot(name, snapshot)
      end

      def delete_download_token(name, snapshot_id)
        snapshot = snapshot_details(name, snapshot_id)
        res = snapshot.delete("token")
        save_snapshot(name, snapshot) if res
      end

      def parse_config
        @config = Yajl::Parser.parse(ENV['WORKER_CONFIG'])
        raise "Need environment variable: WORKER_CONFIG" unless @config
      end

      # Validate the serialized data file.
      # Sub class should override this method to supply specific validation.
      def validate_input(file_path)
        File.open(file_path) do |f|
          return nil unless f.size > 0
        end
        true
      end

      # The name for the saved snapshot file. Subclass can override this method to customize file name.
      def snapshot_filename(name, snapshot_id)
        "#{name}.gz"
      end

      def get_dump_path(name, snapshot_id)
        snapshot_filepath(@config["snapshots_base_dir"], @config["service_name"], name, snapshot_id)
      end
    end

    # Generate download URL for a service snapshot
    class BaseCreateSerializedURLJob < SerializationJob
      VALID_CREDENTIAL_CHARACTERS = ("A".."Z").to_a + ("a".."z").to_a + ("0".."9").to_a

      # workflow template
      def perform
        begin
          required_options :service_id, :snapshot_id
          @name = options["service_id"]
          @snapshot_id = options["snapshot_id"]
          @logger.info("Launch job: #{self.class} for #{name} with options:#{options.inspect}")

          lock = create_lock
          lock.lock do
            result = execute
            @logger.info("Results of create serialized url: #{result}")

            token = generate_download_token()
            update_download_token(name, @snapshot_id, token)
            url = generate_download_url(name, @snapshot_id, token)
            @logger.info("Download link generated for snapshot=#{@snapshot_id} of #{name}: #{url}")

            job_result = { :url => url }
            completed(Yajl::Encoder.encode(job_result))
            @logger.info("Complete job: #{self.class} for #{name}")
          end
        rescue => e
          cleanup(name, @snapshot_id)
          handle_error(e)
        ensure
          set_status({:complete_time => Time.now.to_s})
        end
      end

      # empty
      def execute
        true
      end

      def cleanup(name, snapshot_id)
        return unless (name && snapshot_id)
        begin
          delete_download_token(name, snapshot_id)
        rescue => e
          @logger.error("Error in cleanup: #{e}")
        end
      end

      def generate_download_token(length=12)
        Array.new(length) { VALID_CREDENTIAL_CHARACTERS[rand(VALID_CREDENTIAL_CHARACTERS.length)] }.join
      end

      def generate_download_url(name, snapshot_id, token)
        url_template = @config["download_url_template"]
        url_template % {:service => @config["service_name"], :name => name, :snapshot_id => snapshot_id, :token => token}
      end
    end

    # Create a new snapshot of service using given URL
    class BaseImportFromURLJob < SerializationJob
      attr_reader :url, :snapshot_id

      # Sub class should return true for a successful import job.
      def perform
        begin
          required_options :service_id, :url
          @name = options["service_id"]
          @url = options["url"]
          @logger.info("Launch job: #{self.class} for #{name} with options:#{options.inspect}")

          lock = create_lock
          lock.lock do
            quota = @config["snapshot_quota"]
            if quota
              current = service_snapshots_count(name)
              @logger.debug("Current snapshots count for #{name}: #{current}, max: #{quota}")
              raise ServiceError.new(ServiceError::OVER_QUOTA, name, current, quota) if current >= quota
            end

            @snapshot_id = new_snapshot_id
            @snapshot_path = get_dump_path(name, snapshot_id)
            @snapshot_file = File.join(@snapshot_path, snapshot_filename(name, snapshot_id))

            # clean any data in snapshot folder
            FileUtils.rm_rf(@snapshot_path)
            FileUtils.mkdir_p(@snapshot_path)

            fetch_url(url, @snapshot_file)
            raise ServiceError.new(ServiceError::BAD_SERIALIZED_DATAFILE, url) unless validate_input(@snapshot_file)

            result = execute
            @logger.info("Results of import from url: #{result}")

            snapshot = {
              :snapshot_id => snapshot_id,
              :size => File.open(@snapshot_file) {|f| f.size },
              :date => fmt_time,
              :file => snapshot_filename(name, snapshot_id)
            }
            save_snapshot(name, snapshot)
            @logger.info("Create new snapshot for #{name}:#{snapshot}")

            completed(Yajl::Encoder.encode(filter_keys(snapshot)))
            @logger.info("Complete job: #{self.class} for #{name}")
          end
        rescue => e
          handle_error(e)
          delete_snapshot(name, snapshot_id) if snapshot_id
          FileUtils.rm_rf(@snapshot_path) if @snapshot_path
        ensure
          set_status({:complete_time => Time.now.to_s})
        end
      end

      # Fetch remote uri and stream content to file.
      def fetch_url(url, file_path)
        max_download_size = (@config["serialization"] && @config["serialization"]["max_download_size_mb"] || 10).to_i * 1024 * 1024 # 10M by default
        max_redirects = @config["serialization"] && @config["serialization"]["max_download_redirects"] || 5

        File.open(file_path, "wb+") do |f|
          c = Curl::Easy.new(url)
          # force use ipv4 dns
          c.resolve_mode = :ipv4
          # auto redirect
          c.follow_location = true
          c.max_redirects = max_redirects

          c.on_header do |header|
            if c.downloaded_content_length > max_download_size
              raise ServiceError.new(ServiceError::FILESIZE_TOO_LARGE, url, c.downloaded_content_length, max_download_size)
            end

            header.size
          end

          bytes_downloaded = 0
          c.on_body do |data|
            # calculate bytes downloaded for chucked response
            bytes_downloaded += data.size
            if bytes_downloaded > max_download_size
              raise ServiceError.new(ServiceError::FILESIZE_TOO_LARGE, url, bytes_downloaded, max_download_size)
            end
            f.write(data)
          end

          begin
            c.perform
          rescue Curl::Err::TooManyRedirectsError
            raise ServiceError.new(ServiceError::TOO_MANY_REDIRECTS, url, max_redirects)
          end
        end
      end

      # empty by default
      def execute
        true
      end
    end

    # Create a new snapshot with the given temp file
    class BaseImportFromDataJob < SerializationJob
      attr_reader :temp_file_path, :snapshot_id

      def perform
        begin
          required_options :service_id, :temp_file_path
          @name = options["service_id"]
          @temp_file_path = options["temp_file_path"]
          @logger.info("Launch job: #{self.class} for #{name} with options:#{options.inspect}")

          lock = create_lock
          lock.lock do
            quota = @config["snapshot_quota"]
            if quota
              current = service_snapshots_count(name)
              @logger.debug("Current snapshots count for #{name}: #{current}, max: #{quota}")
              raise ServiceError.new(ServiceError::OVER_QUOTA, name, current, quota) if current >= quota
            end

            raise "Can't find temp file: #{@temp_file_path}" unless File.exists? temp_file_path
            raise ServiceError.new(ServiceError::BAD_SERIALIZED_DATAFILE, "request") unless validate_input(temp_file_path)

            @snapshot_id = new_snapshot_id
            @snapshot_path = get_dump_path(name, snapshot_id)
            @snapshot_file = File.join(@snapshot_path, snapshot_filename(name, snapshot_id))
            # clean any data in snapshot folder
            FileUtils.rm_rf(@snapshot_path)
            FileUtils.mkdir_p(@snapshot_path)

            result = execute
            @logger.info("Results of import from url: #{result}")

            FileUtils.mv(@temp_file_path, @snapshot_file)

            snapshot = {
              :snapshot_id => snapshot_id,
              :size => File.open(@snapshot_file) {|f| f.size },
              :date => fmt_time,
              :file => snapshot_filename(name, snapshot_id)
            }
            save_snapshot(name, snapshot)
            @logger.info("Create new snapshot for #{name}:#{snapshot}")

            completed(Yajl::Encoder.encode(filter_keys(snapshot)))
            @logger.info("Complete job: #{self.class} for #{name}")
          end
        rescue => e
          handle_error(e)
          delete_snapshot(name, snapshot_id) if snapshot_id
          FileUtils.rm_rf(@snapshot_path) if @snapshot_path
        ensure
          set_status({:complete_time => Time.now.to_s})
          FileUtils.rm_rf(@temp_file_path) if @temp_file_path
        end
      end

      # empty
      def execute
        true
      end
    end
  end
end
