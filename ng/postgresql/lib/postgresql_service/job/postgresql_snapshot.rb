# Copyright (c) 2009-2011 VMware, Inc.
require "pg"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..")
require "util"
require "postgresql_error"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "..")
require "postgresql_service/node"

module VCAP::Services::Postgresql::Snapshot
  include VCAP::Services::Base::AsyncJob::Snapshot

  # Dump a database into files and save the snapshot information into redis.
  class CreateSnapshotJob < BaseCreateSnapshotJob

    include VCAP::Services::Postgresql::Util

    def execute
      use_warden = @config['use_warden'] || false

      VCAP::Services::Postgresql::Node.setup_datamapper(:default, @config['local_db'])
      VCAP::Services::Postgresql::Util::PGDBconn.init
      provisionedservice = VCAP::Services::Postgresql::Node::pgProvisionedServiceClass(use_warden).get(name)

      # dump the db and get the dump file size
      dump_file_size = dump_db(provisionedservice, snapshot_id, use_warden)

      # gather the information of the snapshot
      snapshot = {
        :snapshot_id => snapshot_id,
        :size => dump_file_size,
        :files => ["#{snapshot_id}.dump"],
        :manifest => {
          :version => 1,
          :service_version => provisionedservice.version
        }
      }
    end

    def dump_db(provisionedservice, snapshot_id, use_warden)
      name = provisionedservice.name
      # dump file
      dump_path = get_dump_path(name, snapshot_id)
      FileUtils.mkdir_p(dump_path) unless File.exists?(dump_path)
      dump_file_name = File.join(dump_path, "#{snapshot_id}.dump")

      version = provisionedservice.version
      # postgresql's config
      postgres_config = @config['postgresql'][version]
      raise "Can't find configuration for version: #{version}" unless postgres_config

      default_user = provisionedservice.pgbindusers.all(:default_user => true)[0]
      if default_user.nil?
        @logger.error("The provisioned service with name #{name} has no default user")
        raise "Failed to dump database of #{name}"
      end
      user = default_user[:user]
      passwd = default_user[:password]
      host, port, dump_bin = %w(host port dump_bin).map{ |k| postgres_config[k] }
      if use_warden
        host = provisionedservice.ip
      end

      # dump the database
      raise "Failed to dump database #{name}" unless dump_database(
        name, host, port, user, passwd, dump_file_name, :dump_bin => dump_bin)
      dump_file_size = -1
      File.open(dump_file_name) { |f| dump_file_size = f.size }
      # we will return the dump file size
      dump_file_size
    end
  end

  # Rollback data from snapshot files
  class RollbackSnapshotJob < BaseRollbackSnapshotJob

    include VCAP::Services::Postgresql::Util

    def execute
      restore_db(name, snapshot_id)
    end

    def restore_db(name, snapshot_id)
      use_warden = @config['use_warden'] || false
      VCAP::Services::Postgresql::Node.setup_datamapper(:default, @config["local_db"])
      VCAP::Services::Postgresql::Util::PGDBconn.init
      service = VCAP::Services::Postgresql::Node::pgProvisionedServiceClass(use_warden).get(name)
      raise "No information for provisioned service with name #{name}." unless service
      default_user = service.pgbindusers.all(:default_user => true)[0]
      raise "No default user for service #{name}." unless default_user
      version = service.version
      postgres_config = @config["postgresql"][version]

      dump_file_name = @snapshot_files[0]
      raise "Can't find snapshot file #{snapshot_file_path}" unless File.exists?(dump_file_name)

      host, port, vcap_user, vcap_pass, database, restore_bin =
        %w(host port user pass database restore_bin).map{ |k| postgres_config[k]}

      if use_warden
        host = service.ip
      end

      # Need a user who is a superuser to disable db access and then kill all live sessions first
      reset_db(host, port, vcap_user, vcap_pass, database, service)
      # Import the dump file
      parent_user = default_user[:user]
      parent_pass = default_user[:password]
      raise "Failed to restore database #{name}" unless restore_database(
        name, host, port, parent_user, parent_pass, dump_file_name, :restore_bin => restore_bin)
    end

  end
end
