# Copyright (c) 2009-2011 VMware, Inc.
require File.dirname(__FILE__) + "/spec_helper"

require "rabbit_service/rabbit_node"
require "rabbit_service/rabbit_provisioner"
require "rabbit_service/job"
require "mock_redis"

module VCAP
  module Services
    module Rabbit
      class Node
      end
    end
  end
end

describe VCAP::Services::Rabbit::Node do

  before :all do
    @options = getNodeTestConfig
    loadWorkerTestConfig(@options[:local_db_file])
    FileUtils.mkdir_p(@options[:base_dir])
    FileUtils.mkdir_p(@options[:image_dir])
    FileUtils.mkdir_p(@options[:service_log_dir])
    FileUtils.mkdir_p(@options[:migration_nfs])

    # Setup code must be wrapped in EM.run
    EM.run do
      @node = VCAP::Services::Rabbit::Node.new(@options)
      EM.add_timer(1) {EM.stop}
    end
  end

  after :all do
    FileUtils.rm_f(@options[:local_db_file])
    FileUtils.rm_rf(@options[:service_log_dir])
    FileUtils.rm_rf(@options[:image_dir])
    FileUtils.rm_rf(@options[:migration_nfs])
    FileUtils.rm_rf(@options[:base_dir])
    # Use %x to call shell command since ruby doesn't has pkill interface
    %x[pkill epmd]
  end

  describe "Node.snapshot" do
    before :all do
      EM.run do
        @node = VCAP::Services::Rabbit::Node.new(@options)
        @provisioner = VCAP::Services::Rabbit::Provisioner.new({
          :logger => getLogger,
          :plan_management => {
            :plans => {:free => {:low_water => 10}},
          },
          :cc_api_version => 'v2',
        })
        EM.add_timer(1) {EM.stop}
      end
      @credentials = @node.provision(:free)
    end

    after :all do
      @node.unprovision(@credentials["name"])
      @node.shutdown
    end

    before :each do
      r = MockRedis.new
      Redis.stub!(:new).and_return(r)
      # Placeholder for job's config code
      VCAP::Services::Base::AsyncJob::Config.stub!(:redis_config).and_return({:host => '127.0.0.1', :port => 7369})
      VCAP::Services::Base::AsyncJob::Config.stub!(:logger).and_return(getLogger)
      @worker = Resque::Worker.new(*["test_node"])
    end

    it "create rollback and delete snapshot" do
      instance = @node.get_instance(@credentials["name"])
      amqp_new_queue(@credentials, instance, "test_exchange", "test_queue")
      VCAP::Services::Rabbit::Snapshot::CreateSnapshotJob.create(:service_id => @credentials["name"],
                               :node_id => "test_node",
                               :metadata => {
                                 :plan => @options[:plan],
                                 :provider => 'core',
                                 :service_version => @options[:default_version]})
      @worker.process
      snapshots = @provisioner.service_snapshots(@credentials["name"])
      snapshots.size.should == 1
      @snapshot = snapshots[0]
      @credentials["hostname"] = instance.ip
      amqp_clear_queue(@credentials, instance, "test_exchange", "test_queue")
      amqp_exchange_exist?(@credentials, "test_exchange").should be_false
      amqp_queue_exist?(@credentials, "test_queue").should be_false
      VCAP::Services::Rabbit::Snapshot::RollbackSnapshotJob.create(:service_id => @credentials["name"],
                               :node_id => "test_node",
                               :snapshot_id => @snapshot["snapshot_id"])
      @worker.process
      amqp_exchange_exist?(@credentials, "test_exchange").should be_true
      amqp_queue_exist?(@credentials, "test_queue").should be_true

      VCAP::Services::Base::AsyncJob::Snapshot::BaseDeleteSnapshotJob.create(:service_id => @credentials["name"],
                               :node_id => "test_node",
                               :snapshot_id => @snapshot["snapshot_id"])
      @worker.process
      snapshots = @provisioner.service_snapshots(@credentials["name"])
      snapshots.size.should == 0
    end

    it "create serialization url" do
      instance = @node.get_instance(@credentials["name"])
      amqp_new_queue(@credentials, instance, "test_exchange", "test_queue")
      VCAP::Services::Rabbit::Snapshot::CreateSnapshotJob.create(:service_id => @credentials["name"],
                               :node_id => "test_node",
                               :metadata => {
                                 :plan => @options[:plan],
                                 :provider => 'core',
                                 :service_version => @options[:default_version]})
      @worker.process
      snapshots = @provisioner.service_snapshots(@credentials["name"])
      snapshots.size.should == 1
      snapshot_id = snapshots[0]["snapshot_id"]
      VCAP::Services::Base::AsyncJob::Serialization::BaseCreateSerializedURLJob.create(:service_id => @credentials["name"],
                                                                                       :node_id => "test_node",
                                                                                       :snapshot_id => snapshot_id)
      @worker.process
      VCAP::Services::Base::AsyncJob::Snapshot::BaseDeleteSnapshotJob.create(:service_id => @credentials["name"],
                               :node_id => "test_node",
                               :snapshot_id => snapshot_id)
      @worker.process
      snapshots = @provisioner.service_snapshots(@credentials["name"])
      snapshots.size.should == 0
    end

  end

end
