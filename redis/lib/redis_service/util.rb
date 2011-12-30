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

        def dump_redis_data(instance, dump_path, gzip_bin=nil)
          dir = get_config(instance.port, instance.password, "dir")
          set_config(instance.port, instance.password, "dir", dump_path)
          begin
            Timeout::timeout(@redis_timeout) do
              redis = ::Redis.new({:port => instance.port, :password => instance.password})
              redis.save(@save_command_name)
            end
          rescue => e
            raise RedisError.new(RedisError::REDIS_CONNECT_INSTANCE_FAILED)
          ensure
            begin
              set_config(instance.port, instance.password, "dir", dir)
              redis.quit if redis
            rescue => e
            end
          end
          if gzip_bin
            dump_file = File.join(dump_path, "dump.rdb")
            cmd = "#{gzip_bin} -c #{dump_file} > #{dump_path}/#{instance.name}.gz"
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
        end

        def import_redis_data(instance, dump_path, base_dir, redis_server_path, gzip_bin=nil)
          if gzip_bin
            zip_file = File.join(dump_path, "#{instance.name}.gz")
            cmd = "#{gzip_bin} -d #{zip_file}"
            on_err = Proc.new do |cmd, code, msg|
              raise "CMD '#{cmd}' exit with code: #{code}. Message: #{msg}"
            end
            res = CMDHandle.execute(cmd, nil, on_err)
            if res == nil
              return nil
            end
            FileUtils.mv(File.join(dump_path, instance.name), File.join(dump_path, "dump.rdb"))
          end
          config_path = File.join(base_dir, instance.name, "redis.conf")
          dump_file = File.join(dump_path, "dump.rdb")
          stop_redis_server(instance)
          FileUtils.cp(dump_file, File.join(base_dir, instance.name, "data"))
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
        end

        def check_password(port, password)
          Timeout::timeout(@redis_timeout) do
            redis = ::Redis.new({:port => port})
            redis.auth(password)
          end
          true
        rescue => e
          if e.message == "ERR invalid password"
            return false
          else
            raise RedisError.new(RedisError::REDIS_CONNECT_INSTANCE_FAILED)
          end
        ensure
          begin
            redis.quit if redis
          rescue => e
          end
        end

        def get_info(port, password)
          info = nil
          Timeout::timeout(@redis_timeout) do
            redis = ::Redis.new({:port => port, :password => password})
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

        def get_config(port, password, key)
          config = nil
          Timeout::timeout(@redis_timeout) do
            redis = ::Redis.new({:port => port, :password => password})
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

        def set_config(port, password, key, value)
          Timeout::timeout(@redis_timeout) do
            redis = ::Redis.new({:port => port, :password => password})
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

        def stop_redis_server(instance)
          Timeout::timeout(@redis_timeout) do
            redis = ::Redis.new({:port => instance.port, :password => instance.password})
            begin
              redis.shutdown(@shutdown_command_name)
            rescue RuntimeError => e
              # It could be a disabled instance
              if @disable_password
                redis = ::Redis.new({:port => instance.port, :password => @disable_password})
                redis.shutdown(@shutdown_command_name)
              end
            end
          end
        rescue Timeout::Error => e
          @logger.warn(e)
        end

        def close_fds
          3.upto(get_max_open_fd) do |fd|
            begin
              IO.for_fd(fd, "r").close
            rescue
            end
          end
        end

        def get_max_open_fd
          max = 0

          dir = nil
          if File.directory?("/proc/self/fd/") # Linux
            dir = "/proc/self/fd/"
          elsif File.directory?("/dev/fd/") # Mac
            dir = "/dev/fd/"
          end

          if dir
            Dir.foreach(dir) do |entry|
              begin
                pid = Integer(entry)
                max = pid if pid > max
              rescue
              end
            end
          else
            max = 65535
          end

          max
        end
      end
    end
  end
end
