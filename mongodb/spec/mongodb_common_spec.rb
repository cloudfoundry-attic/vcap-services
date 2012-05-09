# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require "spec_helper"
require "mongo"

describe "MongoDB Provisionedservice class" do

  include VCAP::Services::MongoDB

  before :all do
    @options = get_node_config()
    EM.run do
      @node = Node.new(@options)
      EM.add_timer(1) { EM.stop }
    end
  end

  after :all do
    @node.shutdown if @node
    FileUtils.rm_rf(File.dirname(@options[:base_dir])) if @options
  end

  def insert_testdata(p_service)
    con = Mongo::Connection.new(p_service.ip, '27017')
    db = con.db(p_service.db)
    db.authenticate(p_service.admin, p_service.adminpass)
    col = db.collection('testCol')
    col.insert({ 'test_key' => 1234 })
  end

  def update_testdata(p_service)
    con = Mongo::Connection.new(p_service.ip, '27017')
    db = con.db(p_service.db)
    db.authenticate(p_service.admin, p_service.adminpass)
    col = db.collection('testCol')

    col.update({ 'test_key' => 1234 }, { 'test_key' => 5678 })
  end

  def check_testdata(p_service)
    con = Mongo::Connection.new(p_service.ip, '27017')
    db = con.db(p_service.db)
    db.authenticate(p_service.admin, p_service.adminpass)
    col = db.collection('testCol')
    doc = col.find_one()
    doc['test_key'].should == 1234
  end

  it "should be able to create/delete instance" do
    p_service = Node::ProvisionedService.create({ 'port' => 27017 })
    name = p_service.name
    p_service.delete
    p_service = nil
    p_servcie = Node::ProvisionedService.get(name)
    p_service.should be_nil
  end

  context "When a MongoDB instance created and running" do
    before (:each) do
      @p_service = Node::ProvisionedService.create({ 'port' => 27017 })
      @p_service.run
    end

    after (:each) do
      @p_service.stop
      @p_service.delete
    end

    it "should be able to start/stop a instance" do
      lambda { Mongo::Connection.new(@p_service.ip, '27017') }.should_not raise_error
    end

    it "should be able to add admin/user and remove user" do
      lambda {
        @p_service.add_admin(@p_service.admin, @p_service.adminpass)
        @p_service.add_user(@p_service.admin, @p_service.adminpass)
        insert_testdata(@p_service)
        @p_service.remove_user(@p_service.admin)
      }.should_not raise_error
    end

    context "and when admin and user set" do
      before (:each) do
        @p_service.add_admin(@p_service.admin, @p_service.adminpass)
        @p_service.add_user(@p_service.admin, @p_service.adminpass)
      end

      it "should be able to fetch status" do
        @p_service.get_healthz.should == "ok"
        lambda { @p_service.db_stats }.should_not raise_error
        lambda { @p_service.overall_stats }.should_not raise_error
      end

      it "should be able to do repair" do
        insert_testdata(@p_service)
        @p_service.stop
        lambda { @p_service.repair }.should_not raise_error
        @p_service.run
        check_testdata(@p_service)
      end

      it "should be able to dynamic dump data" do
        FileUtils.mkdir_p('/tmp/mongo_backup')
        insert_testdata(@p_service)
        lambda { @p_service.d_dump('/tmp/mongo_backup', false) }.should_not raise_error
        FileUtils.rm_rf('/tmp/mongo_backup')
      end

      it "should be able to dynamic import data" do
        FileUtils.mkdir_p('/tmp/mongo_backup')
        insert_testdata(@p_service)
        @p_service.d_dump('/tmp/mongo_backup', false)
        update_testdata(@p_service)
        lambda { @p_service.d_import('/tmp/mongo_backup') }.should_not raise_error
        check_testdata(@p_service).should be_true
        FileUtils.rm_rf('/tmp/mongo_backup')
      end
    end
  end

  it "should be able to migrate old instance" do
    p_service = Node::ProvisionedService.create({ 'port' => 27017 })
    lambda { Node.sh "umount #{p_service.data_dir}" }.should_not raise_error
    lambda { Node.sh "mkdir -p #{p_service.data_dir}/data" }.should_not raise_error
    lambda { Node.sh "dd if=/dev/zero of=#{p_service.data_dir}/data/admin.ns bs=1M count=68" }.should_not raise_error
    lambda { p_service.to_loopfile }.should_not raise_error
    Dir.exist?(p_service.data_dir+"_bak").should be_true
    File.exist?(p_service.image_file).should be_true
    File.size(p_service.image_file).should be > 100*1024*1024
    FileUtils.rm_rf(p_service.data_dir+"_bak")
    p_service.delete
  end
end
