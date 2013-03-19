# Copyright (c) 2009-2011 VMware, Inc.

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..')
require "util"
require "rabbit_error"
require "datamapper_l"
require "rabbit_node"

BACKUP_NAME = "mnesia.backup"

module VCAP::Services::Rabbit::Snapshot
  include VCAP::Services::Base::AsyncJob::Snapshot

  module Common
    def init_localdb(database_url)
      DataMapper.setup(:default, database_url)
    end

    def init_setting
      options = {}
      ["service_bin_dir", "base_dir", "local_db", "service_log_dir", "image_dir", "service_common_dir"].each do |opt_key|
        options[opt_key.to_sym] = @config[opt_key]
      end
      rabbit_provisioned_service.init(options)
    end

    def rabbit_provisioned_service
      VCAP::Services::Rabbit::Node::ProvisionedService
    end
  end

  # Dump a database into files and save the snapshot information into rabbit.
  class CreateSnapshotJob < BaseCreateSnapshotJob
    include VCAP::Services::Base::Utils
    include VCAP::Services::Rabbit::Util
    include Common

    def execute
      init_localdb(@config["local_db"])
      init_setting
      dump_path = get_dump_path(name, snapshot_id)
      FileUtils.mkdir_p(dump_path)
      filename = "#{snapshot_id}.mnesia"
      dump_file_name = File.join(dump_path, filename)

      srv =  rabbit_provisioned_service.get(name)
      raise "Can't find service instance:#{name}" unless srv
      srv.run_command(srv.container, :script => "#{File.join(srv.erlang_dir, "bin", "escript")} #{File.join(srv.script_dir, "backup_or_restore#{srv.version[0]}.escript")} backup #{srv.name} #{File.join(srv.base_dir, BACKUP_NAME)}")
      FileUtils.cp(File.join(srv.base_dir, BACKUP_NAME), "#{dump_file_name}")

      dump_file_size = -1
      File.open(dump_file_name) {|f| dump_file_size = f.size}
      snapshot = {
        :snapshot_id => snapshot_id,
        :size => dump_file_size,
        :files => [filename],
        :manifest => {
          :version => 1,
          :service_version => srv.version
        }
      }

      snapshot
    end
  end

  # Rollback data from snapshot files.
  class RollbackSnapshotJob < BaseRollbackSnapshotJob
    include VCAP::Services::Base::Utils
    include VCAP::Services::Rabbit::Util
    include Common

    def execute
      init_localdb(@config["local_db"])
      init_setting

      srv = rabbit_provisioned_service.get(name)
      raise "Can't find service instance:#{name}" unless srv

      snapshot_file_path = @snapshot_files[0]
      raise "Can't find snapshot file #{snapshot_file_path}" unless File.exists?(snapshot_file_path)
      manifest = @manifest
      @logger.debug("Manifest for snapshot: #{manifest}")
      # Remove old queue log files
      self.class.sh "rm -rf #{File.join(srv.base_dir, "mnesia", "queues")}"
      # Copy the mnesia backup file
      self.class.sh "cp -f #{snapshot_file_path} #{File.join(srv.base_dir, BACKUP_NAME)}"
      srv.run_command(srv.container, :script => "#{File.join(srv.erlang_dir, "bin", "escript")} #{File.join(srv.script_dir, "backup_or_restore#{srv.version[0]}.escript")} restore #{srv.name} #{File.join(srv.base_dir, BACKUP_NAME)}")

      true
    end
  end
end
