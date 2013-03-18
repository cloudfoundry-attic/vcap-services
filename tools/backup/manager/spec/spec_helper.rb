# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), '..')
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'rubygems'
require 'rspec'
require 'logger'
require 'sinatra'
require 'thin'
require 'fileutils'
require 'vcap_services_base'
require 'backup_manager/manager'

class BackupManagerTests

  def self.create_manager(dirname, target)
    root = File.join(File.dirname(__FILE__), 'test_directories', dirname)
    mgr = ManagerTest.new({
      :logger => Logger.new(STDOUT),
      :wakeup_interval => 3,
      :root => '/',
      :mbus => 'nats://localhost:4222',
      :z_interval => 30,
      :rotation => {
        :max_days => 7,
        :unprovisioned_max_days => 10
      },
      :cleanup => {
        :max_days => 7
      },
      :target => target,
      :enable => true
    })
    mgr.root = root
    mgr
  end

  class ManagerTest < VCAP::Services::Backup::Manager
    attr_accessor :root
    attr_reader :shutdown_invoked

    def initialize(options)
      super(options)
      @tasks = []
      case options[:target]
      when "backups"
        @tasks = [MockRotator.new(self,options[:rotation])]
      when "snapshots"
        @tasks = [MockSnapshotCleaner.new(self, options[:cleanup])]
      when "jobs"
        @tasks = [MockJobCleaner.new(self, options[:job_cleanup])]
      else
        @logger.error("invalid option")
      end

      @shutdown_invoked = false
    end

    def shutdown
      @shutdown_invoked = true
      @logger.debug("shutdown is called")
    end

    def time
      Time.parse("2010-01-20 09:10:20 UTC").to_i
    end
  end

  class MockSnapshotCleaner < VCAP::Services::Backup::SnapshotCleaner
    alias_method :ori_all_cleanup, :all_cleanup
    alias_method :ori_mark_cleanup, :mark_cleanup
    alias_method :ori_keep_mark, :keep_mark
    def all_cleanup(dir, snapshots)
      sleep 2
      @manager.logger.debug("Test all_cleanup #{dir}")
      ori_all_cleanup(dir, snapshots)
    end

    def mark_cleanup(dir, snapshots, latest_key=nil, previous_marktime=nil)
      sleep 2
      @manager.logger.debug("Test mark_cleanup #{dir}")
      ori_mark_cleanup(dir, snapshots, latest_key, previous_marktime)
    end

    def keep_mark(dir, snapshots)
      sleep 1
      @manager.logger.debug("Test keep_marked #{dir}")
      ori_keep_mark(dir, snapshots)
    end

    def rmdashr(path)
    end
  end

  class MockRotator < VCAP::Services::Backup::Rotator
    alias_method :ori_prune,:prune
    alias_method :ori_retain,:retain

    def prune(path, timestamp=nil)
      sleep 2
      @manager.logger.debug("Test prune #{path}")
      ori_prune(path,timestamp)
    end

    def retain(path, timestamp)
      sleep 2
      @manager.logger.debug("Test retain #{path}")
      ori_retain(path,timestamp)
    end

    def rmdashr(path)
    end
  end

  class MockJobCleaner < VCAP::Services::Backup::JobCleaner
  end
end



require 'backup_manager/rotator'
require 'backup_manager/snapshot_cleaner'
require 'backup_manager/job_cleaner'

module BackupWorkerTests

  CC_PORT = 45678
  CCNG_PORT = 45679
  MAX_DAYS = 7
  UNPROVISIONED_MAX_DAYS = 10

  class MockManager
    attr_reader :root, :logger
    def initialize(root, logger)
      @logger = logger
      @root = root
    end
    def time
      Time.parse("2010-01-20 09:10:20 UTC").to_i
    end
    def shutdown?
      false
    end
  end

  class MockCloudController
    def initialize
      @server = Thin::Server.new('localhost', CC_PORT, Handler.new)
    end

    def start
      Thread.new { @server.start }
    end

    def stop
      @server.stop if @server
    end

    class Handler < Sinatra::Base

      get "/services/v1/offerings/:label/handles" do
        case params['label'].gsub!(/-.*$/,'')
        when 'mysql'
          res = Yajl::Encoder.encode({
            :handles => [{
            'service_id' => 'd35b51e7814b34eeeb9bbb3a6b8750755',
            'configuration' => {},
            'credentials' => {}
          }]
          })
        when 'redis'
          res = Yajl::Encoder.encode({
            :handles => [{
            'service_id' => 'f0fe7695-0310-4043-be12-0a09e59e52d0',
            'configuration' => {},
            'credentials' => {}
          }]
          })
        when 'mongodb'
          res = Yajl::Encoder.encode({
            :handles => [{
            'service_id' => 'a7d3b56a-92b4-4e70-8efe-080ae129f83b',
            'configuration' => {},
            'credentials' => {}
          }]
          })
        else
          res = '{}'
        end
        res
      end
    end
  end

  class MockCloudControllerNG
    def initialize
      @serverng = Thin::Server.new('localhost', CCNG_PORT, Handler.new)
    end

    def start
      Thread.new { @serverng.start }
    end

    def stop
      @serverng.stop if @serverng
    end

    class Handler < Sinatra::Base

      get "/services/v1/offerings/:label/handles" do
        case params['label'].gsub!(/-.*$/,'')
        when 'mysql'
          res = Yajl::Encoder.encode({
            :handles => [{
            'service_id' => 'd35b51e7814b34eeeb9bbb3a6b8750755',
            'configuration' => {},
            'credentials' => {}
          }]
          })
        when 'redis'
          res = Yajl::Encoder.encode({
            :handles => [{
            'service_id' => 'f0fe7695-0310-4043-be12-0a09e59e52d0',
            'configuration' => {},
            'credentials' => {}
          }]
          })
        when 'mongodb'
          res = Yajl::Encoder.encode({
            :handles => [{
            'service_id' => 'a7d3b56a-92b4-4e70-8efe-080ae129f83b',
            'configuration' => {},
            'credentials' => {}
          }]
          })
        else
          res = '{}'
        end
        res
      end

      get "/v2/service_instances" do
        res = {}
        if params['inline-relations-depth'] == '2'
          res = {
            :resources => [{
              :entity => {
                :credentials => {
                  :name => 'd35b51e7814b34eeeb9bbb3a6b8750755'
                },
                :service_plan => {
                  :entity => {
                    :service => {
                      :entity => {
                        :label => 'mysql'
                      }
                    }
                  }
                }
              }
            },
            {
              :entity => {
                :credentials => {
                  :name => 'f0fe7695-0310-4043-be12-0a09e59e52d0'
                },
                :service_plan => {
                  :entity => {
                    :service => {
                      :entity => {
                        :label => 'redis'
                      }
                    }
                  }
                }
              }
            },
            {
              :entity => {
                :credentials => {
                  :name => 'a7d3b56a-92b4-4e70-8efe-080ae129f83b'
                },
                :service_plan => {
                  :entity => {
                    :service => {
                      :entity => {
                        :label => 'mongodb'
                      }
                    }
                  }
                }
              }
            }]
          }
        end
        res.to_json
      end
    end
  end
end

class BackupRotatorTests

  include BackupWorkerTests

  def self.create_rotator(root_p, opts)
    logger = Logger.new(STDOUT)
    complicated = root_p == 'complicated' # special handling for this one...
    root = ''
    if complicated
      root = File.join('/tmp','backup_spec','test_directories', 'backups',root_p)
      require 'spec/test_directories/backups/complicated/populate'
      populate_complicated(root)
    else
      root = File.join(File.dirname(__FILE__), 'test_directories', 'backups', root_p)
    end
    manager = MockManager.new(root, logger)
    opts.merge!({:logger => logger})

    yield RotatorTester.new(manager, opts)

    FileUtils.rm_rf(File.join('/tmp', 'backup_spec')) if complicated
  end

  def self.validate_retained(backup, threshold)
    path = backup[0]
    timestamp = backup[1]
    return true if timestamp > threshold
    all_backup = Dir.entries(File.absolute_path('..',path))-['.','..']
    return all_backup.max.to_i == timestamp
  end

  class RotatorTester < VCAP::Services::Backup::Rotator
    alias_method :ori_get_service_ins_v2, :get_service_ins_v2

    def initialize(manager, options)
      super(manager, options)
      @pruned = []
      @retained = []
      @logger = options[:logger]
    end
    def prune(path, timestamp=nil)
      # IMPORTANT: by overriding this method we prevent files in
      # 'test_directories' from actually getting deleted
      @pruned << [path,timestamp]
    end
    def retain(path, timestamp)
      @retained << [path, timestamp]
    end
    def pruned(relpath=nil)
      relpath ? member(@pruned, relpath) : @pruned
    end
    def retained(relpath=nil)
      relpath ? member(@retained, relpath) : @retained
    end
    def member(backups, relpath)
      path = File.join(@manager.root, relpath)
      backups.index { |a| a[0] == path }
    end
    def get_client_auth_token
      return "faketoken"
    end
    def get_service_ins_v2(uri)
      if @options[:cc_enable]
        ori_get_service_ins_v2(uri)
      end
    end
  end
end

class BackupSnapshotCleanerTests

  include BackupWorkerTests

  def self.create_cleaner(root_p, opts)
    logger = Logger.new(STDOUT)
    complicated = root_p == 'complicated' # special handling for this one...
    root = ''
    if complicated
      root = File.join('/tmp','snapshot_spec','test_directories', root_p)
      require 'spec/test_directories/complicated/populate'
      populate_complicated(root)
    else
      root = File.join(File.dirname(__FILE__), 'test_directories', root_p)
    end
    manager = MockManager.new(root, logger)
    opts.merge!({:logger => logger})

    yield SnapshotCleanerTester.new(manager, opts)

    FileUtils.rm_rf(File.join('/tmp', 'snapshot_spec')) if complicated
  end

  def self.validate_all_cleanuped(dir)
    # the dir should not exist
    return Dir.exists?(dir) == false
  end

  def self.validate_keep_marked(dir)
    # the dir should has the last_clean_time file
    if File.exists?(File.join(dir, "last_clean_time"))
      check_file = File.join(dir, "last_clean_time")
      last_clean_time = nil
      previous_marktime = nil
      mark_line = nil
      File.open(check_file, "r") do |file|
        mark_line = file.read()
      end

      if mark_line.nil? == false
        tmp = mark_line.split('|')
        last_clean_time = tmp[0] if tmp.length >= 1
      end
      last_clean_time
    else
      false
    end
  end

  def self.validate_mark_cleanuped(dir)
    # the dir should has the last_clean_time file
    if File.exists?(File.join(dir, "last_clean_time"))
      check_file = File.join(dir, "last_clean_time")
      last_clean_time = nil
      previous_marktime = nil
      mark_line = nil
      File.open(check_file, "r") do |file|
        mark_line = file.read()
      end

      unless mark_line.nil?
        tmp = mark_line.split('|')
        last_clean_time = tmp[0] if tmp.length >= 1
      end
      last_clean_time
    else
      false
    end
  end

  def self.validate_nooped(dir)
    # has no mark file
    File.exists?(dir) && File.exists?(File.join(dir, "last_clean_time")) == false
  end

  class SnapshotCleanerTester < VCAP::Services::Backup::SnapshotCleaner

    alias_method :real_noop, :noop
    alias_method :real_mark_cleanup, :mark_cleanup
    alias_method :real_keep_mark, :keep_mark
    alias_method :real_all_cleanup, :all_cleanup
    alias_method :ori_get_service_ins_v2, :get_service_ins_v2

    def initialize(manager, options)
      super(manager, options)
      @all_cleanuped = []
      @mark_cleanuped = []
      @keep_marked = []
      @nooped = []
      @logger = options[:logger]
    end

    def noop(dir, need_delete_mark)
      @nooped << dir
    end
    def all_cleanup(dir, snapshots)
      # IMPORTANT: by overriding this method we prevent files in
      # 'test_directories' from actually getting deleted
      @all_cleanuped << dir
    end

    def keep_mark(dir, snapshots)
      @keep_marked << dir
    end

    def mark_cleanup(dir, snapshots, latest_key=nil, previous_marktime=nil)
      @mark_cleanuped << dir
    end

    def nooped(relpath=nil)
      relpath ? member(@nooped, relpath) : @nooped
    end

    def all_cleanuped(relpath=nil)
      relpath ? member(@all_cleanuped, relpath) : @all_cleanuped
    end

    def mark_cleanuped(relpath=nil)
      relpath ? member(@mark_cleanuped, relpath) : @mark_cleanuped
    end

    def keep_marked(relpath=nil)
      relpath ? member(@keep_marked, relpath) : @keep_marked
    end

    def member(snapshots, relpath)
      path = File.join(@manager.root, relpath)
      snapshots.index { |a| a == path }
    end

    def get_client_auth_token
      return "faketoken"
    end

    def get_service_ins_v2(uri)
      if @options[:cc_enable]
        ori_get_service_ins_v2(uri)
      end
    end
  end
end

class JobCleanerTests
  MAX_DAYS = 7
  def self.create_cleaner(opts)
    logger = Logger.new(STDOUT)
    manager = MockJobManager.new(logger)
    opts.merge!({:logger => logger})

    yield JobCleanerTester.new(manager, opts, {})
  end
  def time
    Time.parse("2010-01-20 09:10:20 UTC").to_i
  end

  class JobCleanerTester < VCAP::Services::Backup::JobCleaner
    attr_reader :jobs
    def initialize(manager, options, services_redis)
      super(manager, options, services_redis)
      @jobs=[
      {
        :job_id => "1",
        :status => "completed",
        :start_time => "2012-11-29 02:20:58 +0000",
        :description => "None"
      },
      {
        :job_id => "2",
        :status => "queued",
        :start_time => "2002-11-29 02:20:58 +0000",
        :description => "None"
      },
      {
        :job_id => "3",
        :status => "failed",
        :start_time => "2012-11-29 02:20:58 +0000",
        :description => "None"
      },
      {
        :job_id => "4",
        :status => "failed",
        :start_time => "2002-11-29 02:20:58 +0000",
        :description => "None"
      },
      ]
      @timeout_no = 0
      @failed_no = 0
      @logger = options[:logger]
    end

    def get_all_jobs
      job_ids = []
      @jobs.each { |job| job_ids << job[:job_id] }
      job_ids
    end
    def get_job(job_id)
      @jobs.select { |job| job[:job_id] == job_id }.first
    end

    def remove_job(job_id)
      @jobs.delete_if { |job| job[:job_id] == job_id}
    end
  end

  class MockJobManager
    attr_reader :logger
    def initialize(logger)
      @logger = logger
    end
    def time
      Time.parse("2010-01-20 09:10:20 UTC").to_i
    end
    def shutdown?
      false
    end
  end
end
