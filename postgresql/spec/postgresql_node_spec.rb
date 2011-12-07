# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require 'spec_helper'
require 'postgresql_service/node'
require 'postgresql_service/postgresql_error'
require 'pg'
require 'yajl'

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..', 'base', 'lib')
require 'base/service_error'

module VCAP
  module Services
    module Postgresql
      class Node
        attr_reader :connection, :logger, :available_storage
      end
    end
  end
end

module VCAP
  module Services
    module Postgresql
      class PostgresqlError
          attr_reader :error_code
      end
    end
  end
end

describe "Postgresql node normal cases" do
  include VCAP::Services::Postgresql

  before :all do
    @opts = getNodeTestConfig
    @max_db_conns = @opts[:max_db_conns]
    ENV['PGPASSWORD'] = @opts[:postgresql]['pass']
    # Setup code must be wrapped in EM.run
    EM.run do
      @node = Node.new(@opts)
      sleep 1
      EM.add_timer(0.1) {EM.stop}
    end
  end

  before :each do
    @default_plan = "free"
    @default_opts = "default"
    @test_dbs = {}# for cleanup
    # Create one db be default
    @db = @node.provision(@default_plan)
    @db.should_not == nil
    @db["name"].should be
    @db["host"].should be
    @db["host"].should == @db["hostname"]
    @db["port"].should be
    @db["user"].should == @db["username"]
    @db["password"].should be
    @test_dbs[@db] = []
  end

  it "should connect to postgresql database" do
    EM.run do
      expect {@node.connection.query("SELECT 1")}.should_not raise_error
      EM.stop
    end
  end

  it "should restore from backup file" do
    EM.run do
      tmp_db = @node.provision(@default_plan)
      @test_dbs[tmp_db] = []
      conn = connect_to_postgresql(tmp_db)
      conn.query("create table test1(id int)")
      host, port, user, password = %w(host port user pass).map{|key| @opts[:postgresql][key]}
      tmp_file = "/tmp/#{tmp_db['name']}.dump"
      result = `pg_dump -Fc -h #{host} -p #{port} -U #{user} -f #{tmp_file} #{tmp_db['name']}`
      conn.query("drop table test1")
      res = conn.query("select tablename from pg_catalog.pg_tables where schemaname = 'public';")
      res.count.should == 0
      conn.query("create table test2(id int)")
      @node.restore(tmp_db["name"], "/tmp").should == true
      conn = connect_to_postgresql(tmp_db)
      res = conn.query("select tablename from pg_catalog.pg_tables where schemaname = 'public';")
      res.count.should == 1
      res[0]["tablename"].should == "test1"
      FileUtils.rm_rf(tmp_file)
      EM.stop
    end
  end

  it "should be able to disable an instance" do
    EM.run do
      conn = connect_to_postgresql(@db)
      bind_cred = @node.bind(@db["name"],  @default_opts)
      conn2 = connect_to_postgresql(bind_cred)
      @test_dbs[@db] << bind_cred
      @node.disable_instance(@db, [bind_cred])
      expect { conn.query('select 1') }.should raise_error
      expect { conn2.query('select 1') }.should raise_error
      expect { connect_to_postgresql(@db) }.should raise_error
      expect { connect_to_postgresql(bind_cred) }.should raise_error
      EM.stop
    end
  end

  it "should able to dump instance content to file" do
    EM.run do
      conn = connect_to_postgresql(@db)
      conn.query('create table mytesttable(id int)')
      @node.dump_instance(@db, [], '/tmp').should == true
      EM.stop
    end
  end

  it "should recreate database and user when import instance" do
    EM.run do
      db = @node.provision(@default_plan)
      @test_dbs[db] = []
      @node.dump_instance(db, [], '/tmp')
      @node.unprovision(db['name'], [])
      @node.import_instance(db, {}, '/tmp', @default_plan).should == true
      conn = connect_to_postgresql(db)
      expect { conn.query('select 1')}.should_not raise_error
      EM.stop
    end
  end

  it "should recreate bindings when enable instance" do
    EM.run do
      db = @node.provision(@default_plan)
      @test_dbs[db] = []
      binding = @node.bind(db['name'], @default_opts)
      @test_dbs[db] << binding
      conn = connect_to_postgresql(binding)
      @node.disable_instance(db, [binding])
      expect {conn = connect_to_postgresql(binding)}.should raise_error
      expect {conn = connect_to_postgresql(db)}.should raise_error
      value = {
        "fake_service_id" => {
          "credentials" => binding,
          "binding_options" => @default_opts,
        }
      }
      result = @node.enable_instance(db, value)
      result.should be_instance_of Array
      expect {conn = connect_to_postgresql(binding)}.should_not raise_error
      expect {conn = connect_to_postgresql(db)}.should_not raise_error
      EM.stop
    end
  end

  it "should provision a database with correct credential" do
    EM.run do
      @db.should be_instance_of Hash
      conn = connect_to_postgresql(@db)
      expect {conn.query("SELECT 1")}.should_not raise_error
      conn.close if conn
      EM.stop
    end
  end

  it "should prevent user from altering db property" do
    EM.run do
      conn = connect_to_postgresql(@db)
      expect {conn.query("alter database #{@db["name"]} WITH CONNECTION LIMIT 1000")}.should raise_error(PGError, /must be owner of database .*/)
      conn.close if conn
      EM.stop
    end
  end

  it "should return correct instances & bindings list" do
    EM.run do
      before_ins_list = @node.all_instances_list
      tmp_db = @node.provision(@default_plan)
      @test_dbs[tmp_db] = []
      after_ins_list = @node.all_instances_list
      before_ins_list << tmp_db["name"]
      (before_ins_list.sort == after_ins_list.sort).should be_true

      before_bind_list = @node.all_bindings_list
      tmp_bind = @node.bind(tmp_db["name"],  @default_opts)
      @test_dbs[tmp_db] << tmp_bind
      after_bind_list = @node.all_bindings_list
      before_bind_list << tmp_bind
      a, b = [after_bind_list, before_bind_list].map do |list|
        list.map { |item| item["username"] }.sort
      end
      (a == b).should be_true

      EM.stop
    end
  end

  it "should be able to purge the instance & binding from the all_list" do
    EM.run do
      tmp_db = @node.provision(@default_plan)
      ins_list = @node.all_instances_list
      tmp_bind = @node.bind(tmp_db["name"], @default_opts)
      bind_list = @node.all_bindings_list
      oi = ins_list.find { |ins| ins == tmp_db["name"] }
      ob = bind_list.find { |bind| bind["name"] == tmp_bind["name"] and bind["username"] == tmp_bind["username"] }
      oi.should_not be_nil
      ob.should_not be_nil
      expect { @node.unbind(ob) }.should_not raise_error
      expect { @node.unprovision(oi, []) }.should_not raise_error
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
      conn = connect_to_postgresql(@db)
      # should calculate table size
      conn.query("CREATE TABLE test(id INT)")
      conn.query("INSERT INTO test VALUES(10)")
      conn.query("INSERT INTO test VALUES(20)")
      table_size = @node.db_size(@db["name"])
      table_size.should > 0
      # should also calculate index size
      conn.query("CREATE INDEX id_index on test(id)")
      all_size = @node.db_size(@db["name"])
      all_size.should > table_size
      conn.close if conn
      EM.stop
    end

  end

  it "should not create db or send response if receive a malformed request" do
    EM.run do
      db_num = @node.connection.query("select count(*) from pg_database;")[0]['count']
      mal_plan = "not-a-plan"
      db= nil
      expect {
        db=@node.provision(mal_plan)
      }.should raise_error(PostgresqlError, /Invalid plan .*/)
      db.should == nil
      db_num.should == @node.connection.query("select count(*) from pg_database;")[0]['count']
      EM.stop
    end
  end

  it "should not allow old credential to connect if service is unprovisioned" do
    EM.run do
      conn = connect_to_postgresql(@db)
      expect {conn.query("SELECT 1")}.should_not raise_error
      conn.close if conn
      msg = Yajl::Encoder.encode(@db)
      @node.unprovision(@db["name"], [])
      expect {connect_to_postgresql(@db)}.should raise_error
      EM.stop
    end
  end

  it "should return proper error if unprovision a not existing instance" do
    EM.run do
      expect {
        @node.unprovision("not-existing", [])
      }.should raise_error(PostgresqlError, /Postgresql configuration .* not found/)
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
      }.should raise_error(PostgresqlError,/Postgresql configuration .*not found/)

      # no existing credential
      credential = @node.bind(@db["name"],  @default_opts)
      credential.should_not == nil
      @test_dbs[@db] << credential

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
        puts creds
        expect{connect_to_postgresql(creds)}.should raise_error
      end
      EM.stop
    end
  end

  it "should kill long transaction" do
    EM.run do
      # reduce max_long_tx to accelerate test
      @opts[:max_long_tx]=2
      @node = VCAP::Services::Postgresql::Node.new(@opts)
      conn = connect_to_postgresql(@db)
      # prepare a transaction and not commit
      conn.query("create table a(id int)")
      conn.query("insert into a values(10)")
      conn.query("begin")
      conn.query("select * from a for update")
      EM.add_timer(@opts[:max_long_tx]*2) {
        expect {conn.query("select * from a for update")}.should raise_error
        conn.close if conn
        EM.stop
      }
    end
  end

  it "should create a new credential when binding" do
    EM.run do
      binding = @node.bind(@db["name"],  @default_opts)
      binding["name"].should == @db["name"]
      binding["host"].should be
      binding["host"].should == binding["hostname"]
      binding["port"].should be
      binding["user"].should == binding["username"]
      binding["password"].should be
      @test_dbs[@db] << binding
      conn = connect_to_postgresql(binding)
      expect {conn.query("Select 1")}.should_not raise_error
      conn.close if conn
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
      expect {conn = connect_to_postgresql(binding)}.should_not raise_error
      res = @node.unbind(binding)
      res.should be true
      expect {connect_to_postgresql(binding)}.should raise_error
      # old session should be killed
      expect {conn.query("SELECT 1")}.should raise_error
      conn.close if conn
      EM.stop
    end
  end

  it "should delete all bindings if service is unprovisioned" do
    EM.run do
      @default_opts = "default"
      bindings = []
      3.times {bindings << @node.bind(@db["name"], @default_opts)}
      @test_dbs[@db] = bindings
      @node.unprovision(@db["name"], bindings)
      bindings.each {|binding| expect {connect_to_postgresql(binding)}.should raise_error}
      EM.stop
    end
  end

  it "should able to generate varz" do
    EM.run do
      varz = @node.varz_details
      varz.should be_instance_of Hash
      varz[:pg_version].should be
      varz[:db_stat].should be_instance_of Array
      varz[:node_storage_capacity].should > 0
      varz[:node_storage_used].should >= 0
      varz[:long_queries_killed].should >= 0
      varz[:long_transactions_killed].should >= 0
      varz[:provision_served].should >= 0
      varz[:binding_served].should >= 0
      EM.stop
    end
  end

  it "should provide provision/binding served info in varz" do
    EM.run do
      v1 = @node.varz_details
      db = @node.provision(@default_plan)
      binding = @node.bind(db["name"], [])
      @test_dbs[db] = [binding]
      v2 = @node.varz_details
      (v2[:provision_served] - v1[:provision_served]).should == 1
      (v2[:binding_served] - v1[:binding_served]).should == 1
      EM.stop
    end
  end

  it "should report instance disk size in varz" do
    EM.run do
      v = @node.varz_details
      instance = v[:db_stat].find {|d| d[:name] == @db["name"]}
      instance.should_not be_nil
      instance[:size].should >= 0
      EM.stop
    end
  end

  it "should update node capacity after provision new instance" do
    EM.run do
      v1 = @node.varz_details
      db = @node.provision(@default_plan)
      @test_dbs[db] = []
      v2 = @node.varz_details
      (v2[:node_storage_used] - v1[:node_storage_used]).should ==
        (@opts[:max_db_size] * 1024 * 1024)
      @node.unprovision(db["name"], [])
      v3 = @node.varz_details
      (v3[:node_storage_used] - v1[:node_storage_used]).should == 0
      EM.stop
    end
  end

  it "should close extra postgresql connections after generate healthz" do
    EM.run do
      conns_before = {}
      conns_after = {}
      varz = @node.varz_details
      db_stats = varz[:db_stat]
      db_stats.each do |db|
        conns_before[db[:name]] = db[:active_server_processes]
      end
      self_db = db_stats.find {|d| d[:name] == @db["name"]}
      self_db.should_not be_nil

      healthz = @node.healthz_details()
      healthz.keys.size.should >= 2

      sleep 0.1
      varz = @node.varz_details
      db_stats = varz[:db_stat]
      db_stats.each do |db|
        conns_after[db[:name]] = db[:active_server_processes]
      end

      conns_before.each do |k, v|
        conns_after[k].should == v
      end
      EM.stop
    end
  end

  it "should report instance status in healthz" do
    EM.run do
      healthz = @node.healthz_details()
      instance = @db['name']
      healthz[instance.to_sym].should == "ok"
      conn = @node.connection
      conn.query("drop database #{instance}")
      healthz = @node.healthz_details()
      healthz[instance.to_sym].should == "fail"
      # restore db so cleanup code doesn't complain.
      conn.query("create database #{instance}")
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

  after:all do
    ENV['PGPASSWORD'] = ''
    FileUtils.rm_f Dir.glob('/tmp/d*.dump')
  end
end

describe "Postgresql node special cases" do
  include VCAP::Services::Postgresql

  it "should limit max connection to the database" do
    node = nil
    EM.run do
      opts = getNodeTestConfig
      opts[:max_db_conns] = 1
      node = VCAP::Services::Postgresql::Node.new(opts)
      sleep 1
      EM.add_timer(0.1) {EM.stop}
    end
    db = node.provision('free')
    conn = connect_to_postgresql(db)
    expect {conn.query("SELECT 1")}.should_not raise_error
    expect {connect_to_postgresql(db)}.should raise_error(PGError, /too many connections for database .*/)
    conn.close if conn
    node.unprovision(db["name"], [])
  end

  it "should raise error if there is no available storage to provision instance" do
    node = nil
    EM.run do
      opts = getNodeTestConfig
      opts[:available_storage] = 10
      opts[:max_db_size] = 20
      node = VCAP::Services::Postgresql::Node.new(opts)
      sleep 1
      EM.add_timer(0.1) {EM.stop}
    end
    expect {
      node.provision('free')
    }.should raise_error(PostgresqlError, /Node disk is full/)
  end

  it "should handle postgresql error in varz" do
    node = nil
    EM.run do
      opts = getNodeTestConfig
      node = VCAP::Services::Postgresql::Node.new(opts)
      sleep 1
      EM.add_timer(0.1) {EM.stop}
    end
    # drop connection
    node.connection.close
    varz = nil
    expect {varz = node.varz_details}.should_not raise_error
    varz.should == {}
  end

  it "should report node status in healthz" do
    node = nil
    EM.run do
      opts = getNodeTestConfig
      node = VCAP::Services::Postgresql::Node.new(opts)
      sleep 1
      EM.add_timer(0.1) {EM.stop}
    end
    healthz = node.healthz_details()
    healthz[:self].should == "ok"
    node.connection.close
    healthz = node.healthz_details()
    healthz[:self].should == "fail"
  end

  it "should return node not ready if postgresql server is not connected" do
    node = nil
    EM.run do
      opts = getNodeTestConfig
      node = VCAP::Services::Postgresql::Node.new(opts)
      sleep 1
      EM.add_timer(0.1) {EM.stop}
    end
    node.connection.close
    # keep_alive interval is 15 seconds so it should be ok
    node.connection_exception.should be_instance_of PGError
    node.node_ready?.should == false
    node.send_node_announcement.should == nil
  end

  it "should keep alive" do
    node = nil
    EM.run do
      opts = getNodeTestConfig
      node = VCAP::Services::Postgresql::Node.new(opts)
      sleep 1
      EM.add_timer(0.1) {EM.stop}
    end
    node.connection.close
    node.postgresql_keep_alive
    node.node_ready?.should == true
  end
end
