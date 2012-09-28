# Copyright (c) 2009-2011 VMware, Inc.
require "spec_helper"

describe "mongodb_node bind" do

  before :all do
    EM.run do
      @app_id = "myapp"
      @opts = get_node_config()
      @logger = @opts[:logger]
      @default_version = @opts[:default_version]

      @node = Node.new(@opts)
      @resp = @node.provision("free", nil, @default_version)

      EM.add_timer(1) do
        @bind_resp = @node.bind(@resp['name'], 'rw')
        EM.add_timer(1) do
          EM.stop
        end
      end
    end
  end

  it "should have valid response" do
    @resp.should_not be_nil
    @resp['hostname'].should_not be_nil
    @resp['hostname'].should == @bind_resp['host']
    @resp['port'].should_not be_nil
    @resp['username'].should_not be_nil
    @resp['password'].should_not be_nil
    @bind_resp.should_not be_nil
    @bind_resp['hostname'].should_not be_nil
    @bind_resp['hostname'].should == @bind_resp['host']
    @bind_resp['port'].should_not be_nil
    @bind_resp['username'].should_not be_nil
    @bind_resp['password'].should_not be_nil
  end

  it "should be able to connect to mongodb" do
    is_port_open?('127.0.0.1', @resp['port']).should be_true
  end

  it "should return error when tring to bind on non-existed instance" do
    e = nil
    begin
      @node.bind('non-existed', 'rw')
    rescue => e
    end
    e.should_not be_nil
  end

  it "should allow authorized user to access the instance" do
    EM.run do
      conn = Mongo::Connection.new('localhost', @resp['port']).db(@resp['db'])
      auth = conn.authenticate(@bind_resp['username'], @bind_resp['password'])
      auth.should be_true
      coll = conn.collection('mongo_unit_test')
      coll.insert({'a' => 1})
      coll.find()
      coll.count().should == 1
      EM.stop
    end
  end

  it "should not allow unauthorized user to access the instance" do
    EM.run do
      conn = Mongo::Connection.new('localhost', @resp['port']).db(@resp['db'])
      begin
        coll = conn.collection('mongo_unit_test')
        coll.insert({'a' => 1})
        coll.find()
        coll.count().should == 1
      rescue => e
      end
      e.should_not be_nil
      EM.stop
    end
  end

  it "should not allow valid user with empty password to access the instance" do
    EM.run do
      conn = Mongo::Connection.new('localhost', @resp['port']).db(@resp['db'])
      begin
        coll = conn.collection('mongo_unit_test')
        auth = conn.authenticate(@bind_resp['login'], '')
        auth.should be_false
        coll.insert({'a' => 1})
        coll.find()
      rescue => e
      end
      e.should_not be_nil
      EM.stop
    end
  end

  it "should return error when trying to unbind a non-existed service" do
    EM.run do
      begin
        resp  = @node.unbind('not existed')
      rescue => e
      end
      e.should be_true
      EM.add_timer(1) do
        EM.stop
      end
    end
  end

  # unbind here
  it "should be able to unbind it" do
    EM.run do
      resp  = @node.unbind(@bind_resp)
      resp.should be_true
      EM.add_timer(1) do
        EM.stop
      end
    end
  end

  it "should not allow user to access the instance after unbind" do
    EM.run do
      begin
        conn = Mongo::Connection.new('localhost', @resp['port']).db(@resp['db'])
        auth = conn.authenticate(@bind_resp['login'], @bind_resp['secret'])
        auth.should be_false
        coll = conn.collection('mongo_unit_test')
        coll.insert({'a' => 1})
        coll.find()
      rescue => e
        e.should_not be_nil
      end
      EM.stop

    end
  end

  # unprovision here
  it "should be able to unprovision an existing instance" do
    EM.run do
      @node.unprovision(@resp['name'], [])

      e = nil
      begin
        conn = Mongo::Connection.new('localhost', @resp['port']).db('db')
      rescue => e
      end
      e.should_not be_nil
      EM.stop
    end
  end

end


