require "posix/spawn"

module VCAP::Services::Base::Utils
  def self.included(base)
    base.extend(ClassMethods)
  end

  module ClassMethods
    def sh(*args)
      options =
        if args[-1].respond_to?(:to_hash)
          args.pop.to_hash
        else
          {}
        end

      skip_raise = options.delete(:raise) == false
      options = { :timeout => 5.0, :max => 1024 * 1024 }.merge(options)

      status = []
      out_buf = ''
      err_buf = ''
      begin
        pid, iwr, ord, erd = POSIX::Spawn::popen4(*args)
        Timeout::timeout(options[:timeout]) do
          status = Process.waitpid2(pid)
        end
        out_buf += ord.read
        err_buf += erd.read
      rescue => e
        Process.kill("TERM", pid) if pid
        Process.detach(pid)
        raise RuntimeError, "sh #{args} timeout: \nstdout: \n#{out_buf}\nstderr: \n#{err_buf}"
      end

      if status[1].exitstatus != 0
        raise RuntimeError, "sh #{args} failed: \n exit with: #{status[1].exitstatus}\nstdout: \n#{out_buf}\nstderr: \n#{err_buf}" unless skip_raise
      end
      status[1].exitstatus
    end
  end
end
