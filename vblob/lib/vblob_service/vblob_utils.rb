# Copyright (c) 2009-2011 VMware, Inc.

module VCAP
  module Services
    module VBlob
      module Utils

        def service_dir(service_id)
          File.join(@base_dir, service_id)
        end

        def log_dir(instance_id)
          File.join(@vblobd_log_dir,instance_id)
        end

        def log_file_vblob(instance_id)
          File.join(log_dir(instance_id), 'vblob.log')
        end

        def service_exist?(provisioned_service)
          Dir.exists?(service_dir(provisioned_service.name))
        end

        def vblob_dir(base_dir)
          File.join(base_dir,'vblob_data')
        end

        def record_service_log(service_id)
          @logger.warn(" *** BEGIN vblob log - instance: #{service_id}")
          @logger.warn("")
          file = File.new(log_file_vblob(service_id), 'r')
          while (line = file.gets)
            @logger.warn(line.chomp!)
          end
        rescue => e
          @logger.warn(e)
        ensure
          @logger.warn(" *** END vblob log - instance: #{service_id}")
          @logger.warn("")
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
