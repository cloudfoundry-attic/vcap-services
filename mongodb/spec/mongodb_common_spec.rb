# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
$:.unshift(File.join(File.dirname(__FILE__), '..'))
$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require "vcap_services_base"
require "mongodb_service/mongodb_node"
require "mongo"

describe "mongodb service instance" do

  include VCAP::Services::MongoDB

  before :all do
    begin
      options = { :max_memory => 128,
        :base_dir => "/tmp/mongo_data",
        :local_db => "sqlite3:/tmp/mongo_data/mongo.db" }
      Node::ProvisionedService.init(options)
    rescue Exception => e
      raise e
    end
  end

  after :all do
    FileUtils.rm_rf("/tmp/mongo_data")
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

  it "should be able to create/delete new instance" do
    p_service = Node::ProvisionedService.create({ 'port' => 27017 })
    name = p_service.name
    p_service.delete
    p_service = nil
    p_servcie = Node::ProvisionedService.get(name)
    p_service.should be_nil
  end

  it "should be able to start/stop a instance" do
    internal_conn_ok = true
    external_conn_ok = true
    p_service = Node::ProvisionedService.create({ 'port' => 25000 })
    p_service.run
    sleep 1
    begin
      con = Mongo::Connection.new(p_service.ip, '27017')
    rescue Mongo::ConnectionFailure => e
      internal_conn_ok = false
    end
    p_service.stop
    p_service.delete
    internal_conn_ok.should be_true
  end

  it "should be able to add admin/user and remove user" do
    admin_added = false
    user_added = false
    user_removed = false
    p_service = Node::ProvisionedService.create({ 'port' => 27017 })
    p_service.run
    sleep 1
    begin
      p_service.add_admin(p_service.admin, p_service.adminpass)
      admin_added = true
      p_service.add_user(p_service.admin, p_service.adminpass)
      user_added = true
      insert_testdata(p_service)
      p_service.remove_user(p_service.admin)
      user_removed = true
    rescue => e
    end
    p_service.stop
    p_service.delete
    admin_added.should be_true
    user_added.should be_true
    user_removed.should be_true
  end

  it "should be able to fetch status" do
    p_service = Node::ProvisionedService.create({ 'port' => 27017 })
    p_service.run
    sleep 1
    p_service.add_admin(p_service.admin, p_service.adminpass)
    p_service.add_user(p_service.admin, p_service.adminpass)
    insert_testdata(p_service)
    p_service.get_healthz.should == "ok"
    p_service.db_stats
    p_service.overall_stats
    p_service.stop
    p_service.delete
  end

  it "should be able to dynamic dump data" do
    backup_success = false
    p_service = Node::ProvisionedService.create({ 'port' => 27017 })
    p_service.run
    sleep 1
    p_service.add_admin(p_service.admin, p_service.adminpass)
    p_service.add_user(p_service.admin, p_service.adminpass)
    FileUtils.mkdir_p('/tmp/mongo_backup')
    begin
      insert_testdata(p_service)
      p_service.d_dump('/tmp/mongo_backup')
      backup_success = true
    rescue => e
    end
    p_service.stop
    p_service.delete
    FileUtils.rm_rf('/tmp/mongo_backup')
    backup_success.should be_true
  end

  it "should be able to dynamic import data" do
    data_restored = false
    p_service = Node::ProvisionedService.create({ 'port' => 27017 })
    p_service.run
    sleep 1
    p_service.add_admin(p_service.admin, p_service.adminpass)
    p_service.add_user(p_service.admin, p_service.adminpass)
    FileUtils.mkdir_p('/tmp/mongo_backup')
    begin
      insert_testdata(p_service)
      p_service.d_dump('/tmp/mongo_backup', false)
      update_testdata(p_service)
      p_service.d_import('/tmp/mongo_backup')
      data_restored = check_testdata(p_service)
    rescue => e
    end
    p_service.stop
    p_service.delete
    FileUtils.rm_rf('/tmp/mongo_backup')
    data_restored.should be_true
  end

end
