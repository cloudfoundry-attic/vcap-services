# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), '..')
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'rubygems'
require 'rspec'
require 'logger'

require 'backup_manager/manager'

class BackupManagerTests

end

require 'backup_manager/rotator'

class BackupRotatorTests

  MAX_DAYS = 7

  def self.create_rotator(root)
    logger = Logger.new(STDOUT)
    complicated = root == 'complicated' # special handling for this one...
    root = File.join(File.dirname(__FILE__), 'test_directories', root)
    if complicated
      require 'spec/test_directories/complicated/populate'
      populate_complicated(root)
    end
    manager = MockManager.new(root, logger)
    options = {
      :max_days => MAX_DAYS
    }
    RotatorTester.new(manager, options)
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
  end

  class RotatorTester < VCAP::Services::Backup::Rotator
    def initialize(manager, options)
      super(manager, options)
      @pruned = []
      @retained = []
    end
    def prune(path, timestamp)
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

end

