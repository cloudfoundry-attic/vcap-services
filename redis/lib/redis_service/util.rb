# Copyright (c) 2009-2011 VMware, Inc.
require "redis"
$LOAD_PATH.unshift File.join(File.dirname(__FILE__), ".")
require "redis_error"

# Redis client library doesn't support renamed command, so we override the functions here.
class Redis
  def config(config_command_name, action, *args)
    synchronize do
      reply = @client.call [config_command_name.to_sym, action, *args]

      if reply.kind_of?(Array) && action == :get
        Hash[*reply]
      else
        reply
      end
    end
  end

  def shutdown(shutdown_command_name)
    synchronize do
      @client.call [shutdown_command_name.to_sym]
    end
  rescue Errno::ECONNREFUSED => e
    # Since the shutdown is successful, it will raise this connect refused exception by redis client library.
  end

  def save(save_command_name)
    synchronize do
      @client.call [save_command_name.to_sym]
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

        def dump_redis_data(instance, dump_path=nil, gzip_bin=nil, compressed_file_name=nil)
          dir = get_config(instance.ip, @redis_port, instance.password, "dir")
          set_config(instance.ip, @redis_port, instance.password, "dir", dump_path) if dump_path
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
              set_config(instance.ip, @redis_port, instance.password, "dir", dir) if dump_path
              redis.quit if redis
            rescue => e
            end
          end
          if gzip_bin
            dump_file = File.join(dump_path, "dump.rdb")
            cmd = "#{gzip_bin} -c #{dump_file} > #{dump_path}/#{compressed_file_name}"
            on_err = Proc.new do |cmd, code, msg|
              raise "CMD '#{cmd}' exit with code: #{code}. Message: #{msg}"
            end
            res = CMDHandle.execute(cmd, nil, on_err)
            return res
          end
          true
        rescue => e
          @logger.error("Error dump instance #{instance.name}: #{fmt_error(e)}")
          nil
        ensure
          FileUtils.rm(File.join(dump_path, "dump.rdb")) if gzip_bin
        end

        def import_redis_data(instance, dump_path, base_dir, redis_server_path, gzip_bin=nil, compressed_file_name=nil)
          name = instance.name
          dump_file = File.join(dump_path, "dump.rdb")
          temp_file = nil
          if gzip_bin
            # add name in temp file name to prevent file overwritten by other import jobs.
            temp_file = File.join(dump_path, "#{name}.dump.rdb")
            zip_file = File.join(dump_path, "#{compressed_file_name}")
            cmd = "#{gzip_bin} -dc #{zip_file} > #{temp_file}"
            on_err = Proc.new do |cmd, code, msg|
              raise "CMD '#{cmd}' exit with code: #{code}. Message: #{msg}"
            end
            res = CMDHandle.execute(cmd, nil, on_err)
            if res == nil
              return nil
            end
            dump_file = temp_file
          end
          config_path = File.join(base_dir, instance.name, "redis.conf")
          stop_redis_server(instance)
          FileUtils.cp(dump_file, File.join(base_dir, instance.name, "data", "dump.rdb"))
          pid = fork
          if pid
            @logger.debug("Service #{instance.name} started with pid #{pid}")
            # In parent, detch the child.
            Process.detach(pid)
            return pid
          else
            $0 = "Starting Redis instance: #{instance.name}"
            close_fds
            exec("#{redis_server_path} #{config_path}")
          end
          true
        rescue => e
          @logger.error("Failed in import dumpfile to instance #{instance.name}: #{fmt_error(e)}")
          nil
        ensure
          FileUtils.rm_rf temp_file if temp_file
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
