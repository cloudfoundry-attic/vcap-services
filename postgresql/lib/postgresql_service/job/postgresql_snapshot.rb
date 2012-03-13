# Copyright (c) 2009-2011 VMware, Inc.
require "pg"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..")
require "util"
require "postgresql_error"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "..")
require "postgresql_service/node"

module VCAP::Services::Snapshot::Postgresql
  include VCAP::Services::Snapshot

  # Dump a database into files and save the snapshot information into redis.
  class CreateSnapshotJob < SnapshotJob

    include VCAP::Services::Postgresql::Util

    # Resque 's job must implement the perform method
    def perform
      # service id is the provisioned service's id
      name = options["service_id"]
      @logger.info("Begin create snapshot job for: #{name}")

      # get the snapshot id
      VCAP::Services::Snapshot.redis_connect(@config["resque"])
      snapshot_id = get_snapshot_id

      # dump the db and get the dump file size
      dump_file_size = dump_db(name, snapshot_id)

      # gather the information of the snapshot
      complete_time = Time.now
      snapshot = {
        :snapshot_id => snapshot_id,
        :date => complete_time.to_s,
        :size => dump_file_size
      }

      # save the sanpshot info
      save_snapshot(name, snapshot)
      job_result = { :snapshot_id => snapshot_id }
      set_status({ :complete_time=> complete_time.to_s  })
      completed(Yajl::Encoder.encode(job_result))

    rescue => e
      @logger.error("Error in CreateSnapshotJob #{@uuid}:#{fmt_error(e)}")
      cleanup(name, snapshot_id)
      err = (e.instance_of?(ServiceError)? e: ServiceError.new(ServiceError::INTERNAL_ERROR)).to_hash
      err_msg = Yajl::Encoder.encode(err)
      set_status({ :complete_time => Time.now.to_s })
      failed(err_msg)
    end

    def dump_db(name, snapshot_id)
      # dump file
      dump_path = get_dump_path(name, snapshot_id)
      FileUtils.mkdir_p(dump_path) unless File.exists?(dump_path)
      dump_file_name = File.join(dump_path, "#{snapshot_id}.dump")

      # postgresql's config
      postgre_conf = @config['postgresql']

      # setup DataMapper
      VCAP::Services::Postgresql::Node.setup_datamapper(:default, @config['local_db'])
      # prepare the command
      provisionedservice = VCAP::Services::Postgresql::Node::Provisionedservice.get(name)
      default_user = provisionedservice.bindusers.all(:default_user => true)[0]
      if default_user.nil?
        @logger.error("The provisioned service with name #{name} has no default user")
        raise "Failed to dump database of #{name}"
      end
      user = default_user[:user]
      passwd = default_user[:password]
      host, port = %w(host port).map{ |k| postgre_conf[k] }

      # dump the database
      dump_database(name, host, port, user, passwd, dump_file_name ,{ :dump_bin => @config["dump_bin"], :logger => @logger})
      dump_file_size = -1
      File.open(dump_file_name) { |f| dump_file_size = f.size }
      # we will return the dump file size
      dump_file_size
    end
  end

  # Rollback data from snapshot files
  class RollbackSnapshotJob < SnapshotJob

    include VCAP::Services::Postgresql::Util

    def perform
      name = options["service_id"]
      snapshot_id = options["snapshot_id"]
      @logger.info("Begin to rollback snapshot #{snapshot_id} job for #{name} ")

      # try to restore the data
      result = restore_db(name, snapshot_id)
      set_status({ :complete_time => Time.now.to_s })
      completed(Yajl::Encoder.encode( {:result => "ok" } ))

    rescue => e
      @logger.error("Error in Rollback snapshot job #{@uuid}:#{fmt_error(e)}")
      err = ( e.instance_of?(ServiceError)? e: ServiceError.new(ServiceError::INTERNAL_ERROR)).to_hash
      err_msg = Yajl::Encoder.encode(err)
      set_status(:Complete_time => Time.now.to_s)
      failed(err_msg)
    end

    def restore_db(name, snapshot_id)

      VCAP::Services::Postgresql::Node.setup_datamapper(:default, @config["local_db"])
      service = VCAP::Services::Postgresql::Node::Provisionedservice.get(name)
      raise "No information for provisioned service with name #{name}." unless service
      default_user = service.bindusers.all(:default_user => true)[0]
      raise "No default user for service #{name}." unless default_user

      dump_path = get_dump_path(name, snapshot_id)
      dump_file_name = File.join( dump_path, "#{snapshot_id}.dump" )
      raise "Can't find snapshot file #{snapshot_file_path}" unless File.exists?(dump_file_name)

      host, port, vcap_user, vcap_pass = %w(host port user pass).map{ |k| @config["postgresql"][k]}

      # Need a user who is a superuser to disable db access and then kill all live sessions first
      reset_db(host, port, vcap_user, vcap_pass, name, service)
      # Import the dump file
      parent_user = default_user[:user]
      parent_pass = default_user[:password]
      restore_bin = @config["restore_bin"]
      result = restore_database(name, host, port, parent_user, parent_pass, dump_file_name, { :restore_bin => restore_bin, :logger => @logger } )
      result
    end

  end
end
