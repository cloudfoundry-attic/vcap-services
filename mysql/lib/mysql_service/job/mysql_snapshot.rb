# Copyright (c) 2009-2011 VMware, Inc.

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..')
require "util"
require "mysql_error"
require "datamapper_l"

module VCAP::Services::Mysql::Snapshot
  include VCAP::Services::Base::AsyncJob::Snapshot

  module Common
    def init_localdb(database_url)
      DataMapper.setup(:default, database_url)
    end

    def mysql_provisioned_service
      VCAP::Services::Mysql::Node::ProvisionedService
    end
  end

  # Dump a database into files and save the snapshot information into redis.
  class CreateSnapshotJob < BaseCreateSnapshotJob
    include VCAP::Services::Mysql::Util
    include Common

    def execute
      dump_path = get_dump_path(name, snapshot_id)
      FileUtils.mkdir_p(dump_path)
      filename = "#{snapshot_id}.sql.gz"
      dump_file_name = File.join(dump_path, filename)

      mysql_conf = @config["mysql"]
      result = dump_database(name, mysql_conf, dump_file_name, :mysqldump_bin => @config["mysqldump_bin"], :gzip_bin => @config["gzip_bin"])
      raise "Failed to execute dump command to #{name}" unless result

      dump_file_size = -1
      File.open(dump_file_name) {|f| dump_file_size = f.size}
      snapshot = {
        :snapshot_id => snapshot_id,
        :size => dump_file_size,
        :files => [filename],
        :manifest => {
          :version => 1
        }
      }

      snapshot
    end
  end

  # Rollback data from snapshot files.
  class RollbackSnapshotJob < BaseRollbackSnapshotJob
    include VCAP::Services::Mysql::Util
    include Common

    def execute
      init_localdb(@config["local_db"])
      srv = mysql_provisioned_service.get(name)
      instance_user = srv.user
      instance_pass = srv.password

      mysql_conf = @config["mysql"]
      snapshot_file_path = @snapshot_files[0]
      raise "Can't find snapshot file #{snapshot_file_path}" unless File.exists?(snapshot_file_path)
      manifest = @manifest
      @logger.debug("Manifest for snapshot: #{manifest}")

      result = import_dumpfile(name, mysql_conf, instance_user, instance_pass, snapshot_file_path, :mysql_bin => @config["mysql_bin"], :gzip_bin => @config["gzip_bin"])
      raise "Failed execute import command to #{name}" unless result

      true
    end
  end
end
