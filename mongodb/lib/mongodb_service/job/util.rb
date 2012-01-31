# Copyright (c) 2009-2011 VMware, Inc.
require "tmpdir"
require "mongodb_service/mongodb_node"

module VCAP
  module Services
    module MongoDB
      module Util
        def fmt_error(e)
          "#{e}: [#{e.backtrace.join(" | ")}]"
        end

        def make_logger
          return @logger if @logger
          @logger = Logger.new( STDOUT)
          @logger.level = Logger::DEBUG
          @logger
        end

        def dump_database(service_id, file, opts={})
          raise ArgumentError, "Missing options." unless service_id && file
          make_logger

          unless File.exist? @config['local_db'].split(':')[1]
            @logger.error("Could not find local_db: #{@config['local_db']}")
            return 1
          end

          mongodump_path = @config['mongodump_path'] ? @config['mongodump_path'] : 'mongodump'
          tar_path = @config['tar_path'] ? @config['tar_path'] : 'tar'
          cmd_timeout = @config['timeout'].to_f

          DataMapper.setup(:default, @config['local_db'])
          DataMapper::auto_upgrade!

          tmp_dir = Dir.mktmpdir
          service = Node::ProvisionedService.get(service_id)

          commands = [ "#{mongodump_path} -h 127.0.0.1:#{service.port} -u #{service.admin} -p #{service.adminpass} -o #{tmp_dir} ", \
                       "#{tar_path} czf #{file} -C #{tmp_dir} ." ]

          on_err = Proc.new do |cmd, code, msg|
            raise "CMD '#{cmd}' exit with code: #{code}. Message: #{msg}"
          end
          res = -1
          begin
            commands.each do |cmd|
              res = CMDHandle.execute(cmd, nil, on_err)
            end
          ensure
            FileUtils.rm_rf(tmp_dir)
          end
          res
        rescue => e
          @logger.error("Error in CreateSnapshotJob #{service_id}:#{fmt_error(e)}")
          nil
        end

        def restore_database(service_id, file)
          raise ArgumentError, "Missing options." unless service_id && file
          make_logger

          unless File.exist? @config['local_db'].split(':')[1]
            @logger.error("Could not find local_db: #{@config['local_db']}")
            return 1
          end

          mongorestore_path = @config['mongorestore_path'] ? @config['mongorestore_path'] : 'mongorestore'
          tar_path = @config['tar_path'] ? @config['tar_path'] : 'tar'
          cmd_timeout = @config['timeout'].to_f

          DataMapper.setup(:default, @config['local_db'])
          DataMapper::auto_upgrade!

          tmp_dir = Dir.mktmpdir

          service = Node::ProvisionedService.get(service_id)
          db = Mongo::Connection.new('127.0.0.1', service.port).db(service.db)
          db.authenticate(service.admin, service.adminpass)
          db.collection_names.each do |name|
            if name != 'system.users' && name != 'system.indexes'
              db[name].drop
            end
          end

          commands = [ "#{tar_path} xzf #{file} -C #{tmp_dir}", \
                       "#{mongorestore_path} -h 127.0.0.1:#{service.port} -u #{service.admin} -p #{service.adminpass} #{tmp_dir}" ]

          on_err = Proc.new do |cmd, code, msg|
            raise "CMD '#{cmd}' exit with code: #{code}. Message: #{msg}"
          end
          res = -1
          begin
            commands.each do |cmd|
              res = CMDHandle.execute(cmd, nil, on_err)
            end
          ensure
            FileUtils.rm_rf(tmp_dir)
          end
          res
        rescue => e
          @logger.error("Error in RollbackSnapshotJob #{service_id}:#{fmt_error(e)}")
          nil
        end
      end
    end
  end
end

