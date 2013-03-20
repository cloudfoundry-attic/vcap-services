# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require 'spec_helper'

describe BackupRotatorTests do

  before :all do
    @options = {
      :max_days => BackupRotatorTests::MAX_DAYS,
      :unprovisioned_max_days => BackupRotatorTests::UNPROVISIONED_MAX_DAYS
    }
  end

  it "should do nothing with an empty directory" do
    BackupRotatorTests.create_rotator('empty',@options) do |rotator|
      rotator.run.should be_true
      rotator.pruned.length.should == 0
    end
  end

  it "should do nothing with an invalid directory" do
    BackupRotatorTests.create_rotator('invalid',@options) do |rotator|
      rotator.run.should be_true
      rotator.pruned.length.should == 0
    end
  end

  it "should calculate midnghts properly" do
    BackupRotatorTests.create_rotator('empty',@options) do |rotator|
      # manager.time=="2010-01-20 09:10:20 UTC", so...
      rotator.n_midnights_ago(0).should == Time.parse("2010-01-20 00:00:00 UTC").to_i
      rotator.n_midnights_ago(1).should == Time.parse("2010-01-19 00:00:00 UTC").to_i
      rotator.n_midnights_ago(2).should == Time.parse("2010-01-18 00:00:00 UTC").to_i
      rotator.n_midnights_ago(3).should == Time.parse("2010-01-17 00:00:00 UTC").to_i
      rotator.n_midnights_ago(4).should == Time.parse("2010-01-16 00:00:00 UTC").to_i
      rotator.n_midnights_ago(5).should == Time.parse("2010-01-15 00:00:00 UTC").to_i
      rotator.n_midnights_ago(6).should == Time.parse("2010-01-14 00:00:00 UTC").to_i
      rotator.n_midnights_ago(7).should == Time.parse("2010-01-13 00:00:00 UTC").to_i
      rotator.n_midnights_ago(8).should == Time.parse("2010-01-12 00:00:00 UTC").to_i
      rotator.n_midnights_ago(20).should == Time.parse("2009-12-31 00:00:00 UTC").to_i
    end
  end

  it "should bucketize timestamps properly" do
    BackupRotatorTests.create_rotator('empty',@options) do |rotator|
      backups = {
        rotator.n_midnights_ago(0)+10 => "0+10",
        rotator.n_midnights_ago(0)+20 => "0+20",
        rotator.n_midnights_ago(0)+30 => "0+30",
        rotator.n_midnights_ago(1)+10 => "1+10",
        rotator.n_midnights_ago(1)+20 => "1+20",
        rotator.n_midnights_ago(1)+30 => "1+30",
        rotator.n_midnights_ago(2)+10 => "2+10",
        rotator.n_midnights_ago(2)+20 => "2+20",
        rotator.n_midnights_ago(2)+30 => "2+30",
        rotator.n_midnights_ago(3)+10 => "3+10",
        rotator.n_midnights_ago(3)+20 => "3+20",
        rotator.n_midnights_ago(3)+30 => "3+30",
        rotator.n_midnights_ago(4)+10 => "4+10",
        rotator.n_midnights_ago(4)+20 => "4+20",
        rotator.n_midnights_ago(4)+30 => "4+30",
        rotator.n_midnights_ago(5)+10 => "5+10",
        rotator.n_midnights_ago(5)+20 => "5+20",
        rotator.n_midnights_ago(5)+30 => "5+30",
        rotator.n_midnights_ago(6)+10 => "6+10",
        rotator.n_midnights_ago(6)+20 => "6+20",
        rotator.n_midnights_ago(6)+30 => "6+30",
        rotator.n_midnights_ago(7)+10 => "7+10",
        rotator.n_midnights_ago(7)+20 => "7+20",
        rotator.n_midnights_ago(7)+30 => "7+30",
        rotator.n_midnights_ago(8)+10 => "8+10",
        rotator.n_midnights_ago(8)+20 => "8+20",
        rotator.n_midnights_ago(8)+30 => "8+30",
        rotator.n_midnights_ago(9)+10 => "9+10",
        rotator.n_midnights_ago(9)+20 => "9+20",
        rotator.n_midnights_ago(9)+30 => "9+30",
      }
      buckets = rotator.bucketize(backups, @options[:max_days])
      buckets.length.should == 8
      buckets[0].length.should == 3
      buckets[0].include?(rotator.n_midnights_ago(1)+10).should be_true
      buckets[0].include?(rotator.n_midnights_ago(1)+20).should be_true
      buckets[0].include?(rotator.n_midnights_ago(1)+30).should be_true
      buckets[1].length.should == 3
      buckets[1].include?(rotator.n_midnights_ago(2)+10).should be_true
      buckets[1].include?(rotator.n_midnights_ago(2)+20).should be_true
      buckets[1].include?(rotator.n_midnights_ago(2)+30).should be_true
      buckets[2].length.should == 3
      buckets[2].include?(rotator.n_midnights_ago(3)+10).should be_true
      buckets[2].include?(rotator.n_midnights_ago(3)+20).should be_true
      buckets[2].include?(rotator.n_midnights_ago(3)+30).should be_true
      buckets[3].length.should == 3
      buckets[3].include?(rotator.n_midnights_ago(4)+10).should be_true
      buckets[3].include?(rotator.n_midnights_ago(4)+20).should be_true
      buckets[3].include?(rotator.n_midnights_ago(4)+30).should be_true
      buckets[4].length.should == 3
      buckets[4].include?(rotator.n_midnights_ago(5)+10).should be_true
      buckets[4].include?(rotator.n_midnights_ago(5)+20).should be_true
      buckets[4].include?(rotator.n_midnights_ago(5)+30).should be_true
      buckets[5].length.should == 3
      buckets[5].include?(rotator.n_midnights_ago(6)+10).should be_true
      buckets[5].include?(rotator.n_midnights_ago(6)+20).should be_true
      buckets[5].include?(rotator.n_midnights_ago(6)+30).should be_true
      buckets[6].length.should == 3
      buckets[6].include?(rotator.n_midnights_ago(7)+10).should be_true
      buckets[6].include?(rotator.n_midnights_ago(7)+20).should be_true
      buckets[6].include?(rotator.n_midnights_ago(7)+30).should be_true
      buckets[7].length.should == 6
      buckets[7].include?(rotator.n_midnights_ago(8)+10).should be_true
      buckets[7].include?(rotator.n_midnights_ago(8)+20).should be_true
      buckets[7].include?(rotator.n_midnights_ago(8)+30).should be_true
      buckets[7].include?(rotator.n_midnights_ago(9)+10).should be_true
      buckets[7].include?(rotator.n_midnights_ago(9)+20).should be_true
      buckets[7].include?(rotator.n_midnights_ago(9)+30).should be_true
    end
  end

  it "should prune some very old backups but retain the latest backup in it" do
    BackupRotatorTests.create_rotator('one_very_old',@options) do |rotator|
      rotator.run.should be_true
      # mysql/information_schema/, mysql/mysql/ and sampleservice/ab/cd/ef/abcdefghijk/
      rotator.pruned.length.should == 3
      rotator.retained.length.should == 3
      rotator.pruned('sampleservice/ab/cd/ef/abcdefghijk/1134567890').should be_true
      rotator.retained('sampleservice/ab/cd/ef/abcdefghijk/1134567891').should be_true
    end
  end

  it "should not prune a very new backup" do
    BackupRotatorTests.create_rotator('one_very_new',@options) do |rotator|
      rotator.run.should be_true
      rotator.pruned.length.should == 0
    end
  end

  it "should rotate the backup whose handles are not known by CC" do
    EM.run do
      cc = BackupRotatorTests::MockCloudController.new
      cc.start
      EM.add_timer(1) do
        Fiber.new do
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
        BackupRotatorTests.create_rotator('cc_test',opts) do |rotator|
          rotator.run.should be_true
          # retain the unknown backup that is within unprovisioned_max_days (9 days ago)
          rotator.retained('mysql/d1/47/c8/d147c836e304443d1919020da1306a755/1263222000').should be_true
          # prune the unknown backup that is outdated even it is the last one
          rotator.pruned('mongodb/73/12/9c/73129c3a-734e-4f3e-a60e-bdefd371f1e6/1163978520').should be_true
        end
        end.resume
      end
      EM.add_timer(4) do
        cc.stop
        EM.stop
      end
    end
  end


  it "should rotate the backup whose handles are not known by CCNG-v1" do
    EM.run do
      ccng = BackupRotatorTests::MockCloudControllerNG.new
      ccng.start
      EM.add_timer(1) do
        Fiber.new do
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
        BackupRotatorTests.create_rotator('cc_test',opts) do |rotator|
          rotator.run.should be_true
          # retain the unknown backup that is within unprovisioned_max_days (9 days ago)
          rotator.retained('mysql/d1/47/c8/d147c836e304443d1919020da1306a755/1263222000').should be_true
          # prune the unknown backup that is outdated even it is the last one
          rotator.pruned('mongodb/73/12/9c/73129c3a-734e-4f3e-a60e-bdefd371f1e6/1163978520').should be_true
        end
        end.resume
      end
      EM.add_timer(4) do
        ccng.stop
        EM.stop
      end
    end
  end

  it "should rotate the backup whose handles are not known by CCNG-v2" do
    EM.run do
      ccng = BackupRotatorTests::MockCloudControllerNG.new
      ccng.start
      EM.add_timer(1) do
        Fiber.new do
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
        BackupRotatorTests.create_rotator('cc_test',opts) do |rotator|
          rotator.run.should be_true
          # retain the unknown backup that is within unprovisioned_max_days (9 days ago)
          rotator.retained('mysql/d1/47/c8/d147c836e304443d1919020da1306a755/1263222000').should be_true
          # prune the unknown backup that is outdated even it is the last one
          rotator.pruned('mongodb/73/12/9c/73129c3a-734e-4f3e-a60e-bdefd371f1e6/1163978520').should be_true
        end
        end.resume
      end
      EM.add_timer(4) do
        ccng.stop
        EM.stop
      end
    end
  end

  it "should handle a complicated case" do
    BackupRotatorTests.create_rotator('complicated',@options) do |rotator|
      rotator.run.should be_true
      # 'complicated' is a large dataset that was automatically
      # generated by 'test_directories/backups/complicated/populate.rb', so it
      # is impossible to check it in all possible ways.  But here are a
      # few sanity checks.
      # 1. everything should either be retained or pruned
      (rotator.retained.length + rotator.pruned.length).should == SERVICES*INSTANCES_PER_SERVICE*BACKUPS_PER_INSTANCE
      # 2. should discard all backups more than MAX_DAYS old
      # unless the latest backup is out of date too
      # then we just keep the latest one.
      threshold = rotator.n_midnights_ago(BackupRotatorTests::MAX_DAYS)
      rotator.retained.each do |a|
        BackupRotatorTests::validate_retained(a,threshold).should be_true
      end
      # 3. should retain all of today's backups
      midnight = rotator.n_midnights_ago(0)
      rotator.pruned.each do |a|
        a[1].should < midnight
      end
      # 4. should retain at most one backup per day for the MAX_DAYS (except today)
      (1 .. BackupRotatorTests::MAX_DAYS).each do |day|
        service_count = {}
        rotator.retained.each do |a|
          path = a[0]
          timestamp = a[1]
          if timestamp > rotator.n_midnights_ago(day) && timestamp < rotator.n_midnights_ago(day-1)
            path =~ /\A(.*)\/\d+\Z/
              instance = $1
            service_count[instance] = (service_count[instance]||0) + 1
          end
        end
        service_count.values.each do |count|
          count.should < 2 # not "==1" because our random data might not include any backups for the day
        end
      end
    end
  end

end
