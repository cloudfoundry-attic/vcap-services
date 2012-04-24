# Copyright (c) 2009-2011 VMware, Inc.
require "spec_helper"

include VCAP::Services::MongoDB

describe "mongodb rebalance" do
  DUMP_DIR = '/tmp/dump'

  before :each do
    @opts = get_node_config()
    @logger = @opts[:logger]
    EM.run do
      @node = Node.new(@opts)
      EM.add_timer(1) { EM.stop }
    end
  end

  after :each do
    @node.shutdown
    FileUtils.rm_rf(File.dirname(@opts[:base_dir]))
  end

  it "should be able to enable instance" do
    @node.disable_instance(
            @resp,
            { '' => { 'credentials' => @bind_resp } },
          )
    res = @node.enable_instance(
            @resp,
            { '' => { 'credentials' => @bind_resp } },
          )
    res.should == true
    sleep 1
    is_port_open?('127.0.0.1', @resp['port']).should be_true
  end

  it "should be able to disable instance" do
    resp = @node.provision("free")
    p_service = @node.get_instance(resp['name'])
    res = @node.disable_instance(resp, { '' => { 'credentials' => '' } })
    res.should == true
    is_port_open?(p_service.ip, '27017').should be_false
    @node.unprovision(resp['name'], [])
  end

  it "should be able to enable instance" do
    resp = @node.provision("free")
    @node.disable_instance(resp, { '' => { 'credentials' => '' } })
    res = @node.enable_instance(resp, { '' => { 'credentials' => {} } })
    res.should_not be_nil
    p_service = @node.get_instance(resp['name'])
    is_port_open?(p_service.ip, '27017').should be_true
    @node.unprovision(resp['name'], [])
  end

  it "should be able to dump instance" do
    FileUtils.rm_rf(DUMP_DIR)
    resp = @node.provision("free")
    @node.disable_instance(resp, { '' => { 'credentials' => '' } })
    res = @node.dump_instance(resp, { '' => { 'credentials' => {} } }, DUMP_DIR)
    res.should == true
    File.directory?(DUMP_DIR).should be_true
    Dir.entries(DUMP_DIR).size.should > 2
    @node.unprovision(resp['name'], [])
  end

  it "should be able to import instance" do
    resp = @node.provision("free")
    p_service = @node.get_instance(resp['name'])
    conn = Mongo::Connection.new(p_service.ip, '27017')
    db = conn.db(resp['db'])
    auth = db.authenticate(resp['username'], resp['password'])
    auth.should be_true
    coll = db.collection('mongo_unit_test')
    coll.insert({'a' => 1})
    coll.count().should == 1
    @node.disable_instance(resp, { '' => { 'credentials' => '' } })
    @node.dump_instance(resp, { '' => { 'credentials' => {} } }, DUMP_DIR)
    # since the unit test is running on single machine, we delete/unprovision old instance first
    @node.unprovision(resp['name'], [])
    res = @node.import_instance(resp, { '' => { 'credentials' => '' } }, DUMP_DIR, 'free')
    res.should == true
    res = @node.enable_instance(resp, { '' => { 'credentials' => {} } })
    res.should_not be_nil
    p_service = @node.get_instance(resp['name'])
    conn = Mongo::Connection.new(p_service.ip, '27017')
    db = conn.db(resp['db'])
    auth = db.authenticate(resp['username'], resp['password'])
    auth.should be_true
    coll = db.collection('mongo_unit_test')
    doc = coll.find_one()
    doc['a'].should == 1
    @node.unprovision(resp['name'], [])
  end
end


