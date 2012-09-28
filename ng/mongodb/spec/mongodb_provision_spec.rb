# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require "spec_helper"

describe "Mongodb Node" do
  DUMP_DIR = '/tmp/dump'

  before :all do
    @opts = get_node_config()
    @logger = @opts[:logger]
    @default_version = @opts[:default_version]
    @supported_versions = @opts[:supported_versions]
    EM.run do
      @node = Node.new(@opts)
      EM.add_timer(1) { EM.stop }
    end
  end

  after :all do
    @node.shutdown if @node
  end

  it "sh method should return 0 for successful commands" do
    status = Node.sh "ls /"
    status.should == 0
  end

  it "sh method should raise error for failed commands" do
    lambda { Node.sh("ls /abc") }.should raise_error()
  end

  it "should be able to do provision" do
    @resp = @node.provision("free", nil, @default_version)
    @resp.should_not be_nil
    @resp['name'].should_not be_nil
    @resp['name'].should_not == ""
    @resp['hostname'].should_not be_nil
    @resp['port'].should_not be_nil
    @resp['username'].should_not be_nil
    @resp['password'].should_not be_nil
    @node.unprovision(@resp['name'], [])
  end

  context "When a MongoDB instance provisioned" do
    before (:each) do
      @resp = @node.provision("free", nil, @default_version)
      @p_service = @node.get_instance(@resp['name'])
    end

    after (:each) do
      @node.unprovision(@resp['name'], [])
    end

    it "should be able to return varz" do
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

    it "should report error when admin users are deleted from mongodb" do
      pending("will do investigation")
      delete_admin(@p_service, @resp)
      sleep 1
      stats = @node.varz_details
      stats.should_not be_nil
      stats[:running_services].length.should > 0
      stats[:running_services][0]['db'].class.should == String
      stats[:running_services][0]['overall'].class.should == String
    end

    it "should return error when tring to bind on non-existed instance" do
      lambda {
        @node.bind('non-existed', 'rw')
      }.should raise_error
    end

    it "should be able to bind the instance" do
      @bind_resp = @node.bind(@resp['name'], 'rw')
      @bind_resp.should_not be_nil
      @bind_resp['host'].should_not be_nil
      @bind_resp['hostname'].should_not be_nil
      @bind_resp['host'].should == @resp['hostname']
      @bind_resp['hostname'].should == @bind_resp['host']
      @bind_resp['port'].should_not be_nil
      @bind_resp['username'].should_not be_nil
      @bind_resp['password'].should_not be_nil
    end

    it "should return error when trying to unbind a non-existed service" do
      lambda {
        resp  = @node.unbind('not existed')
      }.should raise_error
    end

    it "should be able to unbind it after bind" do
      @bind_resp = @node.bind(@resp['name'], 'rw')
      @node.unbind(@bind_resp).should be_true
      lambda {
        conn = Mongo::Connection.new('localhost', @resp['port'])
        if first_conn_refused
          sleep 1
          first_conn_refused = false
        end
        db = conn.db(@resp['db'])
        auth = db.authenticate(@bind_resp['username'], @bind_resp['password'])
        auth.should be_false
        coll = conn.collection('mongo_unit_test')
        coll.insert({'a' => 1})
        coll.find()
      }.should raise_error
    end

    it "should be able to disable the instance" do
      @node.disable_instance(@resp, { '' => { 'credentials' => '' } }).should be_true
      is_port_open?(@p_service.ip, '27017').should be_false
    end

    it "should be able to enable the instance after disable" do
      @node.disable_instance(@resp, { '' => { 'credentials' => '' } })
      @node.enable_instance(@resp, { '' => { 'credentials' => {} } }).should_not be_nil
      @p_service = @node.get_instance(@resp['name'])
      is_port_open?(@p_service.ip, '27017').should be_true
    end

    it "should be able to dump the instance" do
      FileUtils.rm_rf(DUMP_DIR)
      @node.disable_instance(@resp, { '' => { 'credentials' => '' } })
      @node.dump_instance(@resp, { '' => { 'credentials' => {} } }, DUMP_DIR).should == true
      File.directory?(DUMP_DIR).should be_true
      Dir.entries(DUMP_DIR).size.should > 2
    end

    it "should be able to import the instance" do
      conn = Mongo::Connection.new(@p_service.ip, '27017')
      db = conn.db(@resp['db'])
      auth = db.authenticate(@resp['username'], @resp['password'])
      auth.should be_true
      coll = db.collection('mongo_unit_test')
      coll.insert({'a' => 1})
      coll.count().should == 1
      @node.disable_instance(@resp, { '' => { 'credentials' => '' } })
      @node.dump_instance(@resp, { '' => { 'credentials' => {} } }, DUMP_DIR)
      # since the unit test is running on single machine, we delete/unprovision old instance first
      @node.unprovision(@resp['name'], [])
      @node.import_instance(@resp, { '' => { 'credentials' => '' } }, DUMP_DIR, 'free').should == true
      @node.enable_instance(@resp, { '' => { 'credentials' => {} } }).should_not be_nil
      @p_service = @node.get_instance(@resp['name'])
      conn = Mongo::Connection.new(@p_service.ip, '27017')
      db = conn.db(@resp['db'])
      auth = db.authenticate(@resp['username'], @resp['password'])
      auth.should be_true
      coll = db.collection('mongo_unit_test')
      doc = coll.find_one()
      doc['a'].should == 1
    end
    
    it "should return error when unprovisioning a non-existed instance" do
      lambda {
        @node.unprovision('no existed', [])
      }.should raise_error
    end
  end

  describe "MongoDB provisioned instance" do
    before (:each) do
      @resp = @node.provision("free", nil, @default_version)
      @p_service = @node.get_instance(@resp['name'])
    end

    after (:each) do
      @node.unprovision(@resp['name'], [])
    end

    it "should be able to connect to mongodb" do
      is_port_open?(@p_service.ip, '27017').should be_true
    end

    it "should allow authorized user to access the instance" do
      lambda {
        conn = Mongo::Connection.new(@p_service.ip, '27017')
        db = conn.db(@resp['db'])
        auth = db.authenticate(@resp['username'], @resp['password'])
        auth.should be_true
        coll = db.collection('mongo_unit_test')
        coll.insert({'a' => 1})
        coll.count().should == 1
        conn.close if conn
      }.should_not raise_error
    end

    it "should not allow unauthorized user to access the instance" do
      lambda {
        conn = Mongo::Connection.new(@p_service.ip, '27017')
        db = conn.db(@resp['db'])
        coll = db.collection('mongo_unit_test')
        coll.insert({'a' => 1})
        coll.count()
        conn.close if conn
      }.should raise_error
    end

    it "should enforce no more than max connection to be accepted" do
      pending 'Some version of MongoDB might not ensure max connection'
      first_conn_refused = false
      max_conn_refused = false
      connections = []

      stats = @node.varz_details
      available = stats[:running_services][0]['overall']['connections']['available']

      retry_count = 20
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
  end

  describe "MongoDB provisioned and binded instance" do
    before (:each) do
      @resp = @node.provision("free", nil, @default_version)
      @p_service = @node.get_instance(@resp['name'])
      @bind_resp = @node.bind(@resp['name'], 'rw')
    end

    after (:each) do
      @node.unprovision(@resp['name'], [])
    end

    it "should allow authorized user to access the instance" do
      lambda {
        conn = Mongo::Connection.new(@p_service.ip, '27017')
        db = conn.db(@resp['db'])
        auth = db.authenticate(@bind_resp['username'], @bind_resp['password'])
        auth.should be_true
        coll = db.collection('mongo_unit_test')
        coll.insert({'a' => 1})
        coll.find()
      coll.count().should == 1
      }.should_not raise_error
    end

    it "should not allow unauthorized user to access the instance" do
      lambda {
        conn = Mongo::Connection.new(@p_service.ip, '27017')
        db = conn.db(@resp['db'])
        coll = db.collection('mongo_unit_test')
        coll.insert({'a' => 1})
        coll.find()
        coll.count().should == 1
      }.should raise_error
    end

    it "should not allow valid user with empty password to access the instance" do
      lambda {
        conn = Mongo::Connection.new(@p_service.ip, '27017')
        db = conn.db(@resp['db'])
        coll = db.collection('mongo_unit_test')
        auth = db.authenticate(@bind_resp['username'], '')
        auth.should be_false
        coll.insert({'a' => 1})
        coll.find()
      }.should raise_error
    end
  end

  describe "MongoDB node shutdown/start function" do
    before (:each) do
      @resp = @node.provision("free", nil, @default_version)
    end

    after (:each) do
      @node.unprovision(@resp['name'], [])
    end

    it "should keep the result after node restart" do
      @p_service = @node.get_instance(@resp['name'])

      lambda {
        conn = Mongo::Connection.new(@p_service.ip, '27017')
        db = conn.db(@resp['db'])
        auth = db.authenticate(@resp['username'], @resp['password'])
        auth.should be_true
        coll = db.collection('mongo_unit_test')
        coll.insert({'a' => 1})
        coll.count().should == 1
        conn.close if conn
      }.should_not raise_error

      @node.shutdown

      is_port_open?(@p_service.ip, '27017').should be_false

      @node.pre_send_announcement

      @p_service = @node.get_instance(@resp['name'])

      is_port_open?(@p_service.ip, '27017').should be_true

      lambda {
        conn = Mongo::Connection.new(@p_service.ip, '27017')
        db = conn.db(@resp['db'])
        auth = db.authenticate(@resp['username'], @resp['password'])
        auth.should be_true
        coll = db.collection('mongo_unit_test')
        coll.count().should == 1
        conn.close if conn
      }.should_not raise_error
    end
  end

  describe "MongoDB node multi-version support" do
    it "should allow provision all supported versions" do
      @supported_versions.each do |v|
        rsp = @node.provision("free", nil, v)
        p_service = @node.get_instance(rsp['name'])
        conn = Mongo::Connection.new(p_service.ip, '27017')
        version = conn.server_version.to_s
        conn.close
        version.start_with?(v).should be == true
        @node.unprovision(rsp['name'], [])
      end
    end

    it "should raise exception if unsupported version requested" do
      lambda {
        @node.provision("free", nil, "non_exist")
      }.should raise_error(VCAP::Services::Base::Error::ServiceError.new(VCAP::Services::Base::Error::ServiceError::UNSUPPORTED_VERSION, "non_exist").to_s)
    end
  end
end
