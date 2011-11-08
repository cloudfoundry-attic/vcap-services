require "fileutils"
require "monitor"
require "datamapper"

LOCK_FILE = "/var/vcap/sys/run/LOCK"

# Initialize lockfile
FileUtils.mkdir_p(File.dirname(LOCK_FILE))
File.open(LOCK_FILE, 'w') do |file|
  file.truncate(0)
end

# Export Monitor's count
class Monitor
  def count
    @mon_count
  end
end

module DataMapper

  class GlobalMutex
    def initialize(lockfile)
      @lockfile = lockfile
      @monitor = Monitor.new
    end

    def synchronize
      @monitor.synchronize do
        File.open(@lockfile, 'r') do |file|
          # Only Lock/Unlock on first entrance of synchronize to avoid
          # deadlock on flock
          file.flock(File::LOCK_EX) if @monitor.count == 1
          begin
            yield
          ensure
            file.flock(File::LOCK_UN) if @monitor.count == 1
          end
        end
      end
    end
  end

  LOCK = GlobalMutex.new(LOCK_FILE)

  # The following code will overwrite DataMapper's functions, and replace
  # them with a synchronized version of the same function.
  #
  # NOTICE: some functions will be called twice, for instance, save a resource
  # with foreign keys, so the synchronize function needs to be careful on
  # deadlock.
  module Resource
    alias original_save save
    alias original_destroy destroy

    def save
      LOCK.synchronize do
        original_save
      end
    end

    def destroy
      LOCK.synchronize do
        original_destroy
      end
    end
  end

  module Model
    alias original_get get
    alias original_all all

    def get(*args)
      LOCK.synchronize do
        original_get(*args)
      end
    end

    def all(*args)
      LOCK.synchronize do
        original_all(*args)
      end
    end
  end

  class Collection
    alias original_each each
    alias original_at []

    def each(&block)
      instances = []
      LOCK.synchronize do
        original_each do |instance|
          instances << instance
        end
      end
      instances.each &block
    end

    def [](*args)
      LOCK.synchronize do
        original_at(*args)
      end
    end
  end

  # For auto_upgrade!
  module Migrations
    module SingletonMethods
      alias original_repository_execute repository_execute

      def repository_execute(*args)
        LOCK.synchronize do
          original_repository_execute(*args)
        end
      end
    end
  end

end
