# Copyright (c) 2009-2011 VMware, Inc.
require "redis"
$LOAD_PATH.unshift File.join(File.dirname(__FILE__), ".")
require "redis_error"

# Redis client library doesn't support renamed command, so we override the functions here.
class Redis
  def config(config_command_name, action, *args)
    synchronize do |client|
      client.call [config_command_name.to_sym, action, *args] do |reply|
        if reply.kind_of?(Array) && action == :get
          Hash[*reply]
        else
          reply
        end
      end
    end
  end

  def shutdown(shutdown_command_name)
    synchronize do |client|
      client.with_reconnect(false) do
        begin
          client.call [shutdown_command_name.to_sym]
        rescue ConnectionError
          # This means Redis has probably exited.
          nil
        end
      end
    end
  end

  def save(save_command_name)
    synchronize do |client|
      client.call [save_command_name.to_sym]
    end
  end
end

module VCAP
  module Services
    module Redis
      module Util
        @redis_timeout = 2 if @redis_timeout == nil

        VALID_CREDENTIAL_CHARACTERS = ("A".."Z").to_a + ("a".."z").to_a + ("0".."9").to_a

        def generate_credential(length=12)
          Array.new(length) { VALID_CREDENTIAL_CHARACTERS[rand(VALID_CREDENTIAL_CHARACTERS.length)] }.join
        end

        def fmt_error(e)
          "#{e}: [#{e.backtrace.join(" | ")}]"
        end

        def dump_redis_data(instance, dump_path=nil)
          redis = nil
          begin
            Timeout::timeout(@redis_timeout) do
              redis = ::Redis.new({:host => instance.ip, :port => @redis_port, :password => instance.password})
              redis.save(@save_command_name)
            end
          rescue => e
            raise RedisError.new(RedisError::REDIS_CONNECT_INSTANCE_FAILED)
          ensure
            begin
              redis.quit if redis
            rescue => e
            end
          end
          if dump_path
            FileUtils.cp(File.join(instance.data_dir, "dump.rdb"), dump_path)
          end
          true
        rescue => e
          @logger.error("Error dump instance #{instance.name}: #{fmt_error(e)}")
          nil
        end

        def import_redis_data(instance, dump_path)
          name = instance.name
          dump_file = File.join(dump_path, "dump.rdb")
          instance.stop
          FileUtils.cp(dump_file, instance.data_dir)
          instance.run
          true
        rescue => e
          @logger.error("Failed in import dumpfile to instance #{instance.name}: #{fmt_error(e)}")
          nil
        end

        def get_info(host, port, password)
          info = nil
          redis = nil
          Timeout::timeout(@redis_timeout) do
            redis = ::Redis.new({:host => host, :port => port, :password => password})
            info = redis.info
          end
          info
        rescue => e
          raise RedisError.new(RedisError::REDIS_CONNECT_INSTANCE_FAILED)
        ensure
          begin
            redis.quit if redis
          rescue => e
          end
        end

        def get_config(host, port, password, key)
          config = nil
          redis = nil
          Timeout::timeout(@redis_timeout) do
            redis = ::Redis.new({:host => host, :port => port, :password => password})
            config = redis.config(@config_command_name, :get, key)[key]
          end
          config
        rescue => e
          raise RedisError.new(RedisError::REDIS_CONNECT_INSTANCE_FAILED)
        ensure
          begin
            redis.quit if redis
          rescue => e
          end
        end

        def set_config(host, port, password, key, value)
          redis = nil
          Timeout::timeout(@redis_timeout) do
            redis = ::Redis.new({:host => host, :port => port, :password => password})
            redis.config(@config_command_name, :set, key, value)
          end
        rescue => e
          raise RedisError.new(RedisError::REDIS_CONNECT_INSTANCE_FAILED)
        ensure
          begin
            redis.quit if redis
          rescue => e
          end
        end
      end
    end
  end
end
