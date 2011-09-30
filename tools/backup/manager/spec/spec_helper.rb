# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), '..')
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'rubygems'
require 'rspec'
require 'logger'
require 'sinatra'
require 'thin'
require 'fileutils'
require 'backup_manager/manager'

class BackupManagerTests

  def self.create_manager(dirname)
    root = File.join(File.dirname(__FILE__), 'test_directories', dirname)
    mgr = ManagerTest.new({
      :logger => Logger.new(STDOUT),
      :wakeup_interval => 3,
      :root => '',
      :rotation => {
        :max_days => 7
      },
      :enable => true
    })
    mgr.root = root
    mgr
  end

  class ManagerTest < VCAP::Services::Backup::Manager
    alias_method :ori_run,:run
    attr_accessor :root
    attr_reader :exit_invoked

    def initialize(options)
      super(options)
      @tasks = [MockRotator.new(self,options[:rotation])]
      @exit_invoked = false
    end

    def exit
      @exit_invoked = true
      @logger.debug("exit is called")
    end

    def run
      ori_run
      raise "break the loop"
    end

    def time
      Time.parse("2010-01-20 09:10:20 UTC").to_i
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
end

require 'backup_manager/rotator'


class BackupRotatorTests

  CC_PORT = 45678
  MAX_DAYS = 7

  @@root = ''
  @@complicated = false

  def self.create_rotator(root_p,opts)
    logger = Logger.new(STDOUT)
    @@complicated = root_p == 'complicated' # special handling for this one...
    if @@complicated
      @@root = File.join('/tmp','backup_spec','test_directories',root_p)
      require 'spec/test_directories/complicated/populate'
      populate_complicated(@@root)
    else
      @@root = File.join(File.dirname(__FILE__), 'test_directories', root_p)
    end
    manager = MockManager.new(@@root, logger)
    opts.merge!({:logger => logger})

    RotatorTester.new(manager, opts)
  end

  def self.cleanup
    if @@complicated
      FileUtils.rm_rf(@@root)
    end
  end

  def self.validate_retained(backup,threshold)
    path = backup[0]
    timestamp = backup[1]
    return true if timestamp > threshold
    all_backup = Dir.entries(File.absolute_path('..',path))-['.','..']
    return all_backup.max.to_i == timestamp
  end

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

  class RotatorTester < VCAP::Services::Backup::Rotator
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
end
