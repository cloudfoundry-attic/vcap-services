require "base/backup"

class BackupTest
  class ZombieBackup < VCAP::Services::Base::Backup
    define_method(:default_config_file) {}
    define_method(:backup_db) {}
    attr_reader :exit_invoked

    def initialize
      super
      @exit_invoked = false
    end

    def start
      sleep 3000
    end

    def exit
      @exit_invoked = true
    end
  end
end
