# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require "spec_helper"

describe "Mongodb Node provision/unprovision" do

  before :all do
    @opts = get_node_config()
    @logger = @opts[:logger]
    EM.run do
      @node = Node.new(@opts)
      @resp = @node.provision("free")
      @p_service = @node.get_instance(@resp['name'])
      EM.add_timer(1) { EM.stop }
    end
  end

  after :all do
    @node.shutdown if @node
    FileUtils.rm_rf(File.dirname(@opts[:base_dir]))
  end

  it "should have valid response" do
    @resp.should_not be_nil
    @resp['name'].should_not be_nil
    @resp['name'].should_not == ""
    @resp['hostname'].should_not be_nil
    @resp['port'].should_not be_nil
    @resp['username'].should_not be_nil
    @resp['password'].should_not be_nil
  end

  it "should be able to connect to mongodb" do
    is_port_open?(@p_service.ip, '27017').should be_true
  end

  it "should allow authorized user to access the instance" do
    begin
      conn = Mongo::Connection.new(@p_service.ip, '27017')
      db = conn.db(@resp['db'])
      auth = db.authenticate(@resp['username'], @resp['password'])
      auth.should be_true
      coll = db.collection('mongo_unit_test')
      coll.insert({'a' => 1})
      coll.count().should == 1
    rescue => e
    ensure
      conn.close if conn
    end
    e.should be_nil
  end

  it "should not allow unauthorized user to access the instance" do
    begin
      conn = Mongo::Connection.new(@p_service.ip, '27017')
      db = conn.db(@resp['db'])
      coll = db.collection('mongo_unit_test')
      coll.insert({'a' => 1})
      coll.count()
    rescue Exception => e
      @logger.debug e
    ensure
      conn.close if conn
    end
    e.should_not be_nil
  end

  it "should return varz" do
    stats = nil
    10.times do
      stats = @node.varz_details
    end
    stats.should_not be_nil
    stats[:running_services].length.should > 0
    stats[:running_services][0]['name'].should_not be_nil
    stats[:running_services][0]['db'].should_not be_nil
    stats[:running_services][0]['overall']['connections']['current'].should == 1
    stats[:disk].should_not be_nil
    stats[:max_capacity].should > 0
    stats[:available_capacity].should > 0
    stats[:instances].length.should > 0
  end

  it "should enforce no more than max connection to be accepted" do
    first_conn_refused = false
    max_conn_refused = false
    connections = []

    stats = @node.varz_details
    available = stats[:running_services][0]['overall']['connections']['available']

    # ruby mongo client has a issue. When making connection to mongod, it will make two
    # actual sockets, one for read, one for write. When authentication done, one of them
    # will be closed. But for here, the maximum connection test, it will cause a problem,
    # when last available connection met. So here we decrease one for available connection.
    available = available + 1
    available.times do |i|
      begin
        conn = Mongo::Connection.new(@p_service.ip, '27017')
        db = conn.db(@resp['db'])
        auth = db.authenticate(@resp['username'], @resp['password'])
        connections << conn
      rescue Mongo::ConnectionFailure => e
        first_conn_refused = true
        retry_count -= 1
        retry if ( (i >= (available-1)) && (retry_count > 0))
      end
    end

    # max+1's connection should fail
    begin
      conn = Mongo::Connection.new(@p_service.ip, '27017')
      db = conn.db(@resp['db'])
      auth = db.authenticate(@resp['username'], @resp['password'])
      connections << conn
    rescue Mongo::ConnectionFailure => e
      max_conn_refused = true
    end

    # Close connections
    connections.each do |c|
      c.close
    end

    # Some version of MongoDB might not ensure max connection.
    # For example, MongoDB 1.8 32bits, when set maxConns = 100, it only accepts
    # 99 connections.
    first_conn_refused.should be(false),
    'Some version of MongoDB might not ensure max connection'
    max_conn_refused.should == true
    connections.size.should == available
  end

  it "should report error when admin users are deleted from mongodb" do
    delete_admin(@p_service, @resp)
    stats = @node.varz_details
    stats.should_not be_nil
    stats[:running_services].length.should > 0
    stats[:running_services][0]['db'].class.should == String
    stats[:running_services][0]['overall'].class.should == String
  end

  it "should return error when unprovisioning a non-existed instance" do
    begin
      @node.unprovision('no existed', [])
    rescue => e
    end
    e.should_not be_nil
  end

  # unprovision here
  it "should be able to unprovision an existing instance" do
    @node.unprovision(@resp['name'], [])
    e = is_port_open?(@p_service.ip, '27017')
    e.should be_false
  end
end

describe "Mongodb Node shutdown/start" do

  it "should keep the result after node restart" do
    opts = get_node_config()
    logger = opts[:logger]
    node = nil
    EM.run do
      node = Node.new(opts)
      EM.add_timer(1) { EM.stop }
    end
    resp = node.provision("free")
    p_service = node.get_instance(resp['name'])
    is_port_open?(p_service.ip, '27017').should be_true

    begin
      conn = Mongo::Connection.new(p_service.ip, '27017')
      db = conn.db(resp['db'])
      auth = db.authenticate(resp['username'], resp['password'])
      auth.should be_true
      coll = db.collection('mongo_unit_test')
      coll.insert({'a' => 1})
      coll.count().should == 1
    rescue => e
    ensure
      conn.close if conn
    end

    e.should be_nil

    EM.run do
      node.shutdown
      EM.add_timer(1) { EM.stop }
    end

    is_port_open?(p_service.ip, '27017').should be_false

    EM.run do
      node = Node.new(opts)
      EM.add_timer(1) { EM.stop }
    end

    p_service = node.get_instance(resp['name'])

    is_port_open?(p_service.ip, '27017').should be_true

    begin
      conn = Mongo::Connection.new(p_service.ip, '27017')
      db = conn.db(resp['db'])
      auth = db.authenticate(resp['username'], resp['password'])
      auth.should be_true
      coll = db.collection('mongo_unit_test')
      coll.count().should == 1
    rescue => e
    ensure
      conn.close if conn
    end

    e.should be_nil

    EM.run do
      node.shutdown
      EM.add_timer(1) { EM.stop }
    end

    FileUtils.rm_rf(File.dirname(opts[:base_dir]))
  end
end
