# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require 'spec_helper'

describe BackupSnapshotCleanerTests do

  before :all do
    @options = {
      :max_days => BackupSnapshotCleanerTests::MAX_DAYS
    }
  end

  it "should do nothing with an empty directory (no snapshot directory )" do
    BackupSnapshotCleanerTests.create_cleaner('empty',@options) do |cleaner|
      cleaner.run.should be_true
      cleaner.all_cleanuped.length.should == 0
      cleaner.keep_marked.length.should == 0
      cleaner.mark_cleanuped.length.should == 0
      cleaner.nooped.length.should == 0
    end
  end

  it "should mark a invalid directory" do
    BackupSnapshotCleanerTests.create_cleaner('invalid',@options) do |cleaner|
      cleaner.run.should be_true
      cleaner.all_cleanuped.length.should == 0
      cleaner.keep_marked.length.should == 0
      cleaner.mark_cleanuped.length.should == 2
      cleaner.nooped.length.should == 0
    end
  end

  it "should calculate midnghts properly" do
    BackupSnapshotCleanerTests.create_cleaner('empty',@options) do |cleaner|
      # manager.time=="2010-01-20 09:10:20 UTC", so...
      cleaner.n_midnights_ago(0).should == Time.parse("2010-01-20 00:00:00 UTC").to_i
      cleaner.n_midnights_ago(1).should == Time.parse("2010-01-19 00:00:00 UTC").to_i
      cleaner.n_midnights_ago(2).should == Time.parse("2010-01-18 00:00:00 UTC").to_i
      cleaner.n_midnights_ago(3).should == Time.parse("2010-01-17 00:00:00 UTC").to_i
      cleaner.n_midnights_ago(4).should == Time.parse("2010-01-16 00:00:00 UTC").to_i
      cleaner.n_midnights_ago(5).should == Time.parse("2010-01-15 00:00:00 UTC").to_i
      cleaner.n_midnights_ago(6).should == Time.parse("2010-01-14 00:00:00 UTC").to_i
      cleaner.n_midnights_ago(7).should == Time.parse("2010-01-13 00:00:00 UTC").to_i
      cleaner.n_midnights_ago(8).should == Time.parse("2010-01-12 00:00:00 UTC").to_i
      cleaner.n_midnights_ago(20).should == Time.parse("2009-12-31 00:00:00 UTC").to_i
    end
  end

  it "should try to cleanup the snapshots that CC doesn't know" do
    EM.run do
      cc = BackupRotatorTests::MockCloudController.new
      cc.start
      EM.add_timer(1) do
        Fiber.new {
          opts = @options.merge({
          :cc_api_uri => "localhost:#{BackupRotatorTests::CC_PORT}",
          :cc_api_version => "v1",
          :uaa_client_id => "vmc",
          :uaa_endpoint => "http://uaa.vcap.me",
          :uaa_client_auth_credentials => {
            :username => 'sre@vmware.com',
            :password => 'the_admin_pw'
          },
          :services => {
            'mongodb' => {'version' =>'1.8','token' =>'0xdeadbeef'},
            'redis' => {'version' =>'2.2','token' =>'0xdeadbeef'},
            'mysql' => {'version' =>'5.1','token' =>'0xdeadbeef'},
          },
          :cc_enable => true
        })
        BackupSnapshotCleanerTests.create_cleaner('cc_test',opts) do |cleaner|
          cleaner.run.should be_true
          cleaner.nooped.length.should == 6
          cleaner.mark_cleanuped.length.should == 4
          cleaner.keep_marked.length.should == 0
          cleaner.all_cleanuped.length.should == 1
        end
        }.resume
      end
      EM.add_timer(4) do
        cc.stop
        EM.stop
      end
    end
  end

  it "should try to cleanup the snapshots that CCNG-v1 doesn't know" do
    EM.run do
      ccng = BackupRotatorTests::MockCloudControllerNG.new
      ccng.start
      EM.add_timer(1) do
        Fiber.new {
          opts = @options.merge({
          :cc_api_uri => "localhost:#{BackupRotatorTests::CCNG_PORT}",
          :cc_api_version => "v1",
          :uaa_client_id => "vmc",
          :uaa_endpoint => "http://uaa.vcap.me",
          :uaa_client_auth_credentials => {
            :username => 'sre@vmware.com',
            :password => 'the_admin_pw'
          },
          :services => {
            'mongodb' => {'version' =>'1.8','token' =>'0xdeadbeef'},
            'redis' => {'version' =>'2.2','token' =>'0xdeadbeef'},
            'mysql' => {'version' =>'5.1','token' =>'0xdeadbeef'},
          },
          :cc_enable => true
        })
        BackupSnapshotCleanerTests.create_cleaner('cc_test',opts) do |cleaner|
          cleaner.run.should be_true
          cleaner.nooped.length.should == 6
          cleaner.mark_cleanuped.length.should == 4
          cleaner.keep_marked.length.should == 0
          cleaner.all_cleanuped.length.should == 1
        end
        }.resume
      end
      EM.add_timer(4) do
        ccng.stop
        EM.stop
      end
    end
  end

  it "should try to cleanup the snapshots that CCNG-v2 doesn't know" do
    EM.run do
      ccng = BackupRotatorTests::MockCloudControllerNG.new
      ccng.start
      EM.add_timer(1) do
        Fiber.new {
          opts = @options.merge({
          :cc_api_uri => "localhost:#{BackupRotatorTests::CCNG_PORT}",
          :cc_api_version => "v2",
          :uaa_client_id => "vmc",
          :uaa_endpoint => "http://uaa.vcap.me",
          :uaa_client_auth_credentials => {
            :username => 'sre@vmware.com',
            :password => 'the_admin_pw'
          },
          :services => {
            'mongodb' => {'version' =>'1.8','token' =>'0xdeadbeef'},
            'redis' => {'version' =>'2.2','token' =>'0xdeadbeef'},
            'mysql' => {'version' =>'5.1','token' =>'0xdeadbeef'},
          },
          :cc_enable => true
        })
        BackupSnapshotCleanerTests.create_cleaner('cc_test',opts) do |cleaner|
          cleaner.run.should be_true
          cleaner.nooped.length.should == 6
          cleaner.mark_cleanuped.length.should == 4
          cleaner.keep_marked.length.should == 0
          cleaner.all_cleanuped.length.should == 1
        end
        }.resume
      end
      EM.add_timer(4) do
        ccng.stop
        EM.stop
      end
    end
  end

  it "should handle a complicated case" do

    class BackupSnapshotCleanerTests::SnapshotCleanerTester
      alias_method :ori_get_live_service_ins, :get_live_service_ins
      alias_method :fake_noop, :noop
      alias_method :fake_mark_cleanup, :mark_cleanup
      alias_method :fake_keep_mark, :keep_mark
      alias_method :fake_all_cleanup, :all_cleanup

      def get_live_service_ins
        @serv_ins= {
          "service1" => [],
          "service2" => [],
          "service3" => [],
          "service4" => [],
          "service5" => [],
        }
      end

      def noop(dir, need_delete_mark)
        fake_noop(dir, need_delete_mark)
        real_noop(dir, need_delete_mark)
      end

      def mark_cleanup(dir, snapshots, latest_key=nil, previous_marktime=nil)
        fake_mark_cleanup(dir, snapshots, latest_key, previous_marktime)
        real_mark_cleanup(dir, snapshots, latest_key, previous_marktime)
      end

      def keep_mark(dir, snapshots)
        fake_keep_mark(dir, snapshots)
        real_keep_mark(dir, snapshots)
      end

      def all_cleanup(dir, snapshots)
        fake_all_cleanup(dir, snapshots)
        real_all_cleanup(dir, snapshots)
      end
    end

    BackupSnapshotCleanerTests.create_cleaner('complicated',@options) do |cleaner|
      cleaner.run.should be_true
      # 'complicated' is a large dataset that was automatically
      # generated by 'test_directories/snapshots/complicated/populate.rb', so it
      # is impossible to check it in all possible ways.  But here are a
      # few sanity checks.
      # 1. everything should either be nooped or all_cleanuped or keep_marked or mark_cleanuped
      (cleaner.nooped.length + cleaner.mark_cleanuped.length + cleaner.keep_marked.length + cleaner.all_cleanuped.length).should == SERVICES*(UNMARKED_INSTANCES_PER_SERVICE*SNAPSHOTS_PER_INSTANCE+MARKED_INSTANCES_PER_SERVICE)
      # 2. check nooped
      cleaner.nooped.each do |a|
        BackupSnapshotCleanerTests::validate_nooped(a).should be_true
      end
      # 3. check mark_cleanuped
      cleaner.mark_cleanuped.each do |a|
        BackupSnapshotCleanerTests::validate_mark_cleanuped(a).should be_true
      end
      # 4. check keep_marked
      cleaner.keep_marked.each do |a|
        BackupSnapshotCleanerTests::validate_keep_marked(a).should be_true
      end

      cleaner.all_cleanuped.each do |a|
        BackupSnapshotCleanerTests::validate_all_cleanuped(a).should be_true
      end
    end
  end

end
