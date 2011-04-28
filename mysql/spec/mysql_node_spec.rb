# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require 'spec_helper'
require 'mysql_service/node'
require 'mysql_service/mysql_error'
require 'mysql'
require 'yajl'

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..', 'base', 'lib')
require 'base/service_error'

module VCAP
  module Services
    module Mysql
      class Node
        attr_reader :connection, :logger, :available_storage
      end
    end
  end
end

module VCAP
  module Services
    module Mysql
      class MysqlError
          attr_reader :error_code
      end
    end
  end
end

describe "Mysql server node" do
  include VCAP::Services::Mysql

  before :all do
    @opts = getNodeTestConfig
    # Setup code must be wrapped in EM.run
    EM.run do
      @node = Node.new(@opts)
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
      expect {@node.connection.query("SELECT 1")}.should_not raise_error
      EM.stop
    end
  end

  it "should provison a database with correct credential" do
    EM.run do
      @db.should be_instance_of Hash
      conn = connect_to_mysql(@db)
      expect {conn.query("SELECT 1")}.should_not raise_error
      EM.stop
    end
  end

  it "should calculate available storage correctly" do
    EM.run do
      original= @node.available_storage
      db2 = @node.provision(@default_plan)
      @test_dbs[db2] = []
      current= @node.available_storage
      (original - current).should == @opts[:max_db_size]*1024*1024
      @node.unprovision(db2["name"],[])
      unprov= @node.available_storage
      unprov.should == original
      EM.stop
    end
  end

  it "should calculate both table and index as database size" do
    EM.run do
      conn = connect_to_mysql(@db)
      # should calculate table size
      conn.query("CREATE TABLE test(id INT)")
      conn.query("INSERT INTO test VALUE(10)")
      conn.query("INSERT INTO test VALUE(20)")
      table_size = @node.db_size(@db["name"])
      table_size.should > 0
      # should also calculate index size
      conn.query("CREATE INDEX id_index on test(id)")
      all_size = @node.db_size(@db["name"])
      all_size.should > table_size
      EM.stop
    end

  end

  it "should not create db or send response if receive a malformed request" do
    EM.run do
      db_num = @node.connection.query("show databases;").num_rows()
      mal_plan = "not-a-plan"
      db= nil
      expect {
        db=@node.provision(mal_plan)
      }.should raise_error(MysqlError, /Invalid plan .*/)
      db.should == nil
      db_num.should == @node.connection.query("show databases;").num_rows()
      EM.stop
    end
  end

  it "should raise error if there is no available storage to provision instance" do
    EM.run do
      @opts[:available_storage]=10
      @opts[:max_db_size]=20
      @node = VCAP::Services::Mysql::Node.new(@opts)
      expect {
        @node.provision(@default_plan)
      }.should raise_error(MysqlError, /Node disk is full/)
      EM.stop
    end
  end

  it "should not allow old credential to connect if service is unprovisioned" do
    EM.run do
      conn = connect_to_mysql(@db)
      expect {conn.query("SELECT 1")}.should_not raise_error
      msg = Yajl::Encoder.encode(@db)
      @node.unprovision(@db["name"], [])
      expect {connect_to_mysql(@db)}.should raise_error
      error = nil
      EM.stop
    end
  end

  it "should return proper error if unprovision a not existing instance" do
    EM.run do
      expect {
        @node.unprovision("not-existing", [])
      }.should raise_error(MysqlError, /Mysql configuration .* not found/)
      # nil input handle
      @node.unprovision(nil, []).should == nil
      EM.stop
    end
  end

  it "should return proper error if unbind a not existing credential" do
    EM.run do
      # no existing instance
      expect {
        @node.unbind({:name => "not-existing"})
      }.should raise_error(MysqlError,/Mysql configuration .*not found/)

      # no existing credential
      credential = @node.bind(@db["name"],  @default_opts)
      credential.should_not == nil
      @test_dbs[@db] << credential
      invalid_credential = credential.dup
      invalid_credential["password"] = 'fake'
      expect {
        @node.unbind(invalid_credential)
      }.should raise_error(MysqlError, /Mysql credential .* not found/)

      # nil input
      @node.unbind(nil).should == nil
      EM.stop
    end
  end

  it "should not be possible to access one database using null or wrong credential" do
    EM.run do
      plan = "free"
      db2= @node.provision(plan)
      @test_dbs[db2] = []
      fake_creds = []
      3.times {fake_creds << @db.clone}
      # try to login other's db
      fake_creds[0]["name"] = db2["name"]
      # try to login using null credential
      fake_creds[1]["password"] = nil
      # try to login using root account
      fake_creds[2]["user"] = "root"
      fake_creds.each do |creds|
        expect{connect_to_mysql(creds)}.should raise_error
      end
      EM.stop
    end
  end

  it "should kill long transaction" do
    EM.run do
      # reduce max_long_tx to accelerate test
      @opts[:max_long_tx]=2
      @node = VCAP::Services::Mysql::Node.new(@opts)
      conn = connect_to_mysql(@db)
      # prepare a transaction and not commit
      conn.query("create table a(id int) engine=innodb")
      conn.query("insert into a value(10)")
      conn.query("begin")
      conn.query("select * from a for update")
      EM.add_timer(@opts[:max_long_tx]*2) {
        expect {conn.query("select * from a for update")}.should raise_error
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
      expect {conn.query("Select 1")}.should_not raise_error
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
      expect {conn = connect_to_mysql(binding)}.should_not raise_error
      res = @node.unbind(binding)
      res.should be true
      expect {connect_to_mysql(binding_res)}.should raise_error
      # old session should be killed
      expect {conn.query("SELECT 1")}.should raise_error
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
      bindings.each { |binding| expect {connect_to_mysql(binding)}.should raise_error }
      EM.stop
    end
  end

  after:each do
    @test_dbs.keys.each do |db|
      begin
        name = db["name"]
        @node.unprovision(name, @test_dbs[db])
        @node.logger.info("Clean up temp database: #{name}")
      rescue => e
        @node.logger.info("Error during cleanup #{e}")
      end
    end if @test_dbs
  end
end
