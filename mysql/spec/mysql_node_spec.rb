# Copyright (c) 2009-2011 VMware, Inc.
require 'spec_helper'
require 'mysql_service/node'
require 'mysql'
require 'yajl'

module VCAP
  module Services
    module Mysql
      class Node
        attr_reader :connection, :logger
      end
    end
  end
end

describe "Mysql server node" do

  before :all do
    @opts = getNodeTestConfig
    # Setup code must be wrapped in EM.run
    EM.run do
      @node = VCAP::Services::Mysql::Node.new(@opts)
      EM.stop
    end
  end

  before :each do
    @default_plan = "free"
    @default_opts = "default"
    @test_dbs = {}# for cleanup
    # Create one db be default
    @db = @node.provision(@default_plan)
    @db.should_not == nil
    @test_dbs[@db] = []
  end

  it "should connect to mysql database" do
    EM.run do
      lambda {@node.connection.query("SELECT 1")}.should_not raise_error
      EM.stop
    end
  end

  it "should provison a database with correct credential" do
    EM.run do
      @db.should be_instance_of Hash
      conn = connect_to_mysql(@db)
      lambda {conn.query("SELECT 1")}.should_not raise_error
      EM.stop
    end
  end

  it "should not create db or send response if receive a malformed request" do
    EM.run do
      db_num = @node.connection.query("show databases;").num_rows()
      mal_plan = "not-a-plan"
      db= nil
      lambda {db=@node.provision(mal_plan)}.should_not raise_error
      db.should == nil
      db_num.should == @node.connection.query("show databases;").num_rows()
      EM.stop
    end
  end

  it "should not allow old credential to connect if service is unprovisioned" do
    EM.run do
      conn = connect_to_mysql(@db)
      lambda {conn.query("SELECT 1")}.should_not raise_error
      msg = Yajl::Encoder.encode(@db)
      @node.unprovision(@db["name"], [])
      lambda {connect_to_mysql(@db)}.should raise_error
      error = nil
      EM.stop
    end
  end

  it "should not be possible to access one database with another one's credentials " do
    EM.run do
      plan = "free"
      db2= @node.provision(plan)
      @test_dbs[db2] = []
      db1 = @db.clone
      db1["name"] = db2["name"]
      lambda {connect_to_mysql(db1)}.should raise_error
      EM.stop
    end
  end

  it "should kill long query" do
    pending "TODO: figure out how to write a slow query in test..."
    EM.run do
      EM.stop
    end
  end

  it "should kill long transaction" do
    EM.run do
      @node = VCAP::Services::Mysql::Node.new(@opts)
      conn = connect_to_mysql(@db)
      # prepare a transaction and not commit
      conn.query("create table a(id int) engine=innodb")
      conn.query("insert into a value(10)")
      conn.query("begin")
      conn.query("select * from a for update")
      EM.add_timer(@opts[:max_long_tx]*2) {
        lambda {conn.query("select * from a for update")}.should raise_error
        EM.stop
      }
    end
  end

  it "should create new a credential when binding" do
    EM.run do
      binding = @node.bind(@db["name"],  @default_opts)
      binding["name"].should == @db["name"]
      @test_dbs[@db] << binding
      conn = connect_to_mysql(binding)
      lambda {conn.query("Select 1")}.should_not raise_error
      # nil input
      @node.bind(nil, nil).should == nil
      EM.stop
    end
  end

  it "should supply different credentials when binding evoked with the same input" do
    EM.run do
      binding = @node.bind(@db["name"], @default_opts)
      binding2 = @node.bind(@db["name"], @default_opts)
      @test_dbs[@db] << binding
      @test_dbs[@db] << binding2
      binding.should_not == binding2
      EM.stop
    end
  end

  it "should delete credential after unbinding" do
    EM.run do
      binding = @node.bind(@db["name"], @default_opts)
      @test_dbs[@db] << binding
      conn = nil
      lambda {conn = connect_to_mysql(binding)}.should_not raise_error
      res = @node.unbind(binding)
      res.should be true
      lambda {connect_to_mysql(binding_res)}.should raise_error
      # old session should be killed
      lambda {conn.query("SELECT 1")}.should raise_error
      # handle nil input
      @node.unbind(nil).should == nil
      EM.stop
    end
  end

  it "should delete all bindings if service is unprovisioned" do
    EM.run do
      @default_opts = "default"
      bindings = []
      3.times { bindings << @node.bind(@db["name"], @default_opts)}
      @test_dbs[@db] = bindings
      conn = nil
      @node.unprovision(@db["name"], bindings)
      bindings.each { |binding| lambda {connect_to_mysql(binding)}.should raise_error }
      EM.stop
    end
  end

  after:each do
    @test_dbs.keys.each do |db|
      begin
        name = db["name"]
        @node.unprovision(name, @test_dbs[db])
        @node.logger.info("Clean up temp database: #{name}")
      rescue Mysql::Error => e
        @node.logger.error("Error during cleanup #{e.error}")
      end
    end if @test_dbs
  end
end
