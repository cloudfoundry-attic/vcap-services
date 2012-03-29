# Copyright (c) 2009-2011 VMware, Inc.
require "spec_helper"

describe "Mongodb Node bind/unbind" do

  before :all do
    @opts = get_node_config()
    @logger = @opts[:logger]
    EM.run do
      @node = Node.new(@opts)
      @resp = @node.provision("free")
      @p_service = @node.get_instance(@resp['name'])
      @bind_resp = @node.bind(@resp['name'], 'rw')
      EM.add_timer(1) { EM.stop }
    end
  end

  after :all do
    @node.unprovision(@resp['name'], [])
    @node.shutdown
    FileUtils.rm_rf(File.dirname(@opts[:base_dir]))
  end

  it "should have valid response" do
    @bind_resp.should_not be_nil
    @bind_resp['host'].should_not be_nil
    @bind_resp['hostname'].should_not be_nil
    @bind_resp['host'].should == @resp['hostname']
    @bind_resp['hostname'].should == @bind_resp['host']
    @bind_resp['port'].should_not be_nil
    @bind_resp['username'].should_not be_nil
    @bind_resp['password'].should_not be_nil
  end

  it "should allow authorized user to access the instance" do
    conn = Mongo::Connection.new(@p_service.ip, '27017')
    db = conn.db(@resp['db'])
    auth = db.authenticate(@bind_resp['username'], @bind_resp['password'])
    auth.should be_true
    coll = db.collection('mongo_unit_test')
    coll.insert({'a' => 1})
    coll.find()
    coll.count().should == 1
  end

  it "should not allow unauthorized user to access the instance" do
    conn = Mongo::Connection.new(@p_service.ip, '27017')
    db = conn.db(@resp['db'])
    begin
      coll = db.collection('mongo_unit_test')
      coll.insert({'a' => 1})
      coll.find()
      coll.count().should == 1
    rescue => e
    end
    e.should_not be_nil
  end

  it "should not allow valid user with empty password to access the instance" do
    conn = Mongo::Connection.new(@p_service.ip, '27017')
    db = conn.db(@resp['db'])
    begin
      coll = db.collection('mongo_unit_test')
      auth = db.authenticate(@bind_resp['username'], '')
      auth.should be_false
      coll.insert({'a' => 1})
      coll.find()
    rescue => e
    end
    e.should_not be_nil
  end

  it "should return error when tring to bind on non-existed instance" do
    begin
      @node.bind('non-existed', 'rw')
    rescue => e
    end
    e.should_not be_nil
  end

  it "should return error when trying to unbind a non-existed service" do
    begin
      resp  = @node.unbind('not existed')
    rescue => e
    end
    e.should be_true
  end

  # unbind here
  it "should be able to unbind it" do
    resp  = @node.unbind(@bind_resp)
    resp.should be_true
    begin
      conn = Mongo::Connection.new('localhost', @resp['port'])
      db = conn.db(@resp['db'])
      auth = db.authenticate(@bind_resp['username'], @bind_resp['password'])
      auth.should be_false
      coll = conn.collection('mongo_unit_test')
      coll.insert({'a' => 1})
      coll.find()
    rescue => e
        e.should_not be_nil
    end
  end
end


