# Copyright (c) 2009-2011 VMware, Inc.

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..')
require "util"
require "mysql_error"
require "datamapper_l"
require "node"

module VCAP::Services::Mysql::Snapshot
  include VCAP::Services::Base::AsyncJob::Snapshot

  module Common
    def init_localdb(database_url)
      DataMapper.setup(:default, database_url)
    end

    def mysql_provisioned_service(use_warden)
      VCAP::Services::Mysql::Node.mysqlProvisionedServiceClass(use_warden)
    end
  end

  # Dump a database into files and save the snapshot information into redis.
  class CreateSnapshotJob < BaseCreateSnapshotJob
    include VCAP::Services::Mysql::Util
    include Common

    def execute
      use_warden = @config["use_warden"] || false
      dump_path = get_dump_path(name, snapshot_id)
      FileUtils.mkdir_p(dump_path)
      filename = "#{snapshot_id}.sql.gz"
      dump_file_name = File.join(dump_path, filename)

      init_localdb(@config["local_db"])
      srv =  mysql_provisioned_service(use_warden).get(name)
      raise "Can't find service instance:#{name}" unless srv
      mysql_conf = @config["mysql"][srv.version]
      mysql_conf["host"] = srv.ip if use_warden

      result = dump_database(name, mysql_conf, dump_file_name, :mysqldump_bin => mysql_conf["mysqldump_bin"], :gzip_bin => @config["gzip_bin"])
      raise "Failed to execute dump command to #{name}" unless result

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
    include VCAP::Services::Mysql::Util
    include Common

    def execute
      init_localdb(@config["local_db"])
      use_warden = @config["use_warden"] || false

      srv = mysql_provisioned_service(use_warden).get(name)
      raise "Can't find service instance:#{name}" unless srv
      mysql_conf = @config["mysql"][srv.version]
      mysql_conf["host"] = srv.ip if use_warden
      instance_user = srv.user
      instance_pass = srv.password

      snapshot_file_path = @snapshot_files[0]
      raise "Can't find snapshot file #{snapshot_file_path}" unless File.exists?(snapshot_file_path)
      manifest = @manifest
      @logger.debug("Manifest for snapshot: #{manifest}")

      result = import_dumpfile(name, mysql_conf, instance_user, instance_pass, snapshot_file_path, :mysql_bin => mysql_conf["mysql_bin"], :gzip_bin => @config["gzip_bin"])
      raise "Failed execute import command to #{name}" unless result

      true
    end
  end
end
