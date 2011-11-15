# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require "spec_helper"

describe "mongodb_node provision" do

  before :all do
    EM.run do
      @opts = get_node_config()
      @logger = @opts[:logger]
      @node = Node.new(@opts)
      @original_memory = @node.available_memory

      EM.add_timer(2) { @resp = @node.provision("free") }
      EM.add_timer(4) { EM.stop }
    end
  end

  it "should have valid response" do
    @resp.should_not be_nil
    puts @resp
    inst_name = @resp['name']
    inst_name.should_not be_nil
    inst_name.should_not == ""
  end

  it "should consume node's memory" do
    (@original_memory - @node.available_memory).should > 0
  end

  it "should be able to connect to mongodb" do
    is_port_open?('127.0.0.1', @resp['port']).should be_true
  end

  it "should not allow unauthorized user to access the instance" do
    EM.run do
      begin
        conn = Mongo::Connection.new('localhost', @resp['port'])
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
      EM.stop
    end
  end

  it "should return varz" do
    EM.run do
      stats = nil
      10.times do
        stats = @node.varz_details
        @node.healthz_details
      end
      stats.should_not be_nil
      stats[:running_services].length.should > 0
      stats[:running_services][0]['name'].should_not be_nil
      stats[:running_services][0]['db'].should_not be_nil
      stats[:running_services][0]['overall']['connections']['current'].should == 1
      stats[:disk].should_not be_nil
      stats[:services_max_memory].should > 0
      stats[:services_used_memory].should > 0
      EM.stop
    end
  end

  it "should return healthz" do
    EM.run do
      stats = @node.healthz_details
      stats.should_not be_nil
      stats[:self].should == "ok"
      stats[@resp['name'].to_sym].should == "ok"
      EM.stop
    end
  end

  it "should allow authorized user to access the instance" do
    EM.run do
      begin
        conn = Mongo::Connection.new('localhost', @resp['port'])
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
      EM.stop
    end
  end

  it "should keep the result after node restart" do
    port_open_1 = nil
    port_open_2 = nil
    EM.run do
      EM.add_timer(0) { @node.shutdown }
      EM.add_timer(1) { port_open_1 = is_port_open?('127.0.0.1', @resp['port']) }
      EM.add_timer(2) { @node = Node.new(@opts) }
      EM.add_timer(3) { port_open_2 = is_port_open?('127.0.0.1', @resp['port']) }
      EM.add_timer(4) { EM.stop }
    end

    begin
      port_open_1.should be_false
      port_open_2.should be_true
      conn = Mongo::Connection.new('localhost', @resp['port'])
      db = conn.db(@resp['db'])
      auth = db.authenticate(@resp['username'], @resp['password'])
      auth.should be_true
      coll = db.collection('mongo_unit_test')
      coll.count().should == 1
    rescue => e
    ensure
      conn.close if conn
    end
  end

  it "should enforce no more than max connection to be accepted" do
    conn_refused = false
    connections = []

    # By default, mongodb ensures there're no more than 819 connections
    # for each instance. But it seems for mongodb-1.8.1-32bit, less than
    # 819 connections can be created because of the "can't create new
    # thread, closing connection" error. However, as long as the max number
    # of 819 is enforced, we are good with it.
    900.times do
      begin
        connections << Mongo::Connection.new('localhost', @resp['port'])
      rescue Mongo::ConnectionFailure => e
        conn_refused = true
      end
    end

    # Close connections
    connections.each do |c|
      c.close
    end

    connections.size.should <= 819
    conn_refused.should == true
  end

  it "should return error when unprovisioning a non-existed instance" do
    EM.run do
      e = nil
      begin
        @node.unprovision('no existed', [])
      rescue => e
      end
      e.should_not be_nil
      EM.stop
    end
  end

  it "should report error when admin users are deleted from mongodb" do
    EM.run do
      delete_admin(@resp)
      stats = @node.varz_details
      stats.should_not be_nil
      stats[:running_services].length.should > 0
      stats[:running_services][0]['db'].class.should == String
      stats[:running_services][0]['overall'].class.should == String
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

  it "should release memory" do
    EM.run do
      @original_memory.should == @node.available_memory
      EM.stop
    end
  end

end


