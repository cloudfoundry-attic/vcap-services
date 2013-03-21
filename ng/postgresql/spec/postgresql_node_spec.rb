# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require 'spec_helper'
require 'postgresql_service/node'
require 'postgresql_service/postgresql_error'
require 'pg'
require 'yajl'

module VCAP
  module Services
    module Postgresql
      class Node
        attr_reader :connection, :connections, :discarded_connections, :logger, :available_storage, :provision_served, :binding_served
        def get_service(db)
          pgProvisionedService.first(:name => db['name'])
        end
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
    @opts.freeze

    @default_plan = "free"
    @default_version = @opts[:default_version]
    @default_opts = "default"
    @max_db_conns = @opts[:max_db_conns]
    # Setup code must be wrapped in EM.run
    EM.run do
      @node = VCAP::Services::Postgresql::Node.new(@opts)
      EM.add_timer(0.1) {EM.stop}
    end
  end

  before :each do
    @test_dbs = {}# for cleanup
    # Create one db be default
    @db = @node.provision(@default_plan, nil, @default_version)
    @db.should_not == nil
    @db["name"].should be
    @db["host"].should be
    @db["host"].should == @db["hostname"]
    @db["port"].should be
    @db["user"].should == @db["username"]
    @db["password"].should be
    @test_dbs[@db] = []
    @new_test_dbs ={}
    @db_instance = @node.pgProvisionedService.get(@db['name'])
  end

  it "should connect to postgresql database" do
    EM.run do
      expect {@node.global_connection(@db_instance).query("SELECT 1")}.should_not raise_error
      @node.get_inst_port(@db_instance).should == @db['port']
      EM.stop
    end
  end

  it "should add timeout to connect" do
    pending "Not to use warden, won't run this case." unless @opts[:use_warden]
    pending "Not to use async methods, won't run this case." unless @opts[:db_use_async_query]
    EM.run do
      default_connect_timeout = VCAP::Services::Postgresql::Util::PGDBconn.default_connect_timeout

      tmp_db = @node.provision(@default_plan, nil, @default_version)
      tmp_instance = @node.pgProvisionedService.get(tmp_db['name'])
      @test_dbs[tmp_db] = []

      expect { connect_to_postgresql(tmp_db).close }.should_not raise_error

      begin
        # simulate the select timeout issue
        class IO
          class << self
            alias_method :select_ori, :select
            def select(read_array, write_array, error_array, timeout)
                # block thread for speficied time if timeout is set
                # or block thread for ever (180 seconds is long enough for test)
                sleep timeout || 180
                raise PGError, "simulated select timeout"
            end
          end
        end

        # use default connect_timeout
        start_time = Time.now.to_f
        expect { conn = connect_to_postgresql(tmp_db) }.should raise_error(PGError, /simulated select timeout/)
        connect_time = Time.now.to_f - start_time
        connect_time.should >= default_connect_timeout
        connect_time.should < (default_connect_timeout + 1)

        # use specified connect_timeout
        start_time = Time.now.to_f
        expect { conn = connect_to_postgresql(tmp_db, :connect_timeout => (default_connect_timeout + 2))}.should raise_error(PGError, /simulated select timeout/)
        connect_time = Time.now.to_f - start_time
        connect_time >= default_connect_timeout + 2
        connect_time.should < (default_connect_timeout + 3)

      ensure
        # restore the IO.select method
        class IO
          class << self
            alias_method :select, :select_ori
          end
        end
      end

      EM.stop
    end
  end

  it "should retrieve version from postgresql connection" do
    EM.run do
      begin
        @conn = @node.global_connection(@db_instance)
        @node.pg_version(@conn).should == @default_version
        @node.pg_version(@conn, :full => true).should =~ Regexp.new("#{@default_version}\\.(\\w+)")
        @node.pg_version(@conn, :major => true).should == @default_version.scan(/(\d+)/)[0][0]
      ensure
        EM.stop
      end
    end
  end

  it "should add timeout to query" do
    EM.run do
      ori_default_query_timeout = VCAP::Services::Postgresql::Util::PGDBconn.default_query_timeout
      opts = @opts.dup
      opts[:db_query_timeout] = 1
      opts[:not_start_instances] = true if opts[:use_warden]
      node = VCAP::Services::Postgresql::Node.new(opts)
      EM.add_timer(2) do
        begin
          @conn = @node.global_connection(@db_instance)
          conn = node.global_connection(@db_instance)
          # make sure the connections are active
          expect {@conn.query('select current_timestamp')}.should_not raise_error
          expect {conn.query('select current_timestamp')}.should_not raise_error
          @conn.query_timeout.should == ori_default_query_timeout
          conn.query_timeout.should == 1
          expect {@conn.query('select pg_sleep(2)')}.should_not raise_error
          expect {conn.query('select pg_sleep(2)')}.should raise_error
        ensure
          VCAP::Services::Postgresql::Util::PGDBconn.default_query_timeout = ori_default_query_timeout
          EM.stop
        end
      end
    end
  end

  it "should support async query/transaction" do
    NUM = 10
    TEST_TIME = 5
    EM.run do
      ori_use_async_query = VCAP::Services::Postgresql::Util::PGDBconn.use_async_query
      opts = @opts.dup
      opts[:db_use_async_query] = true
      opts[:not_start_instances] = true if opts[:use_warden]
      node = VCAP::Services::Postgresql::Node.new(opts)
      EM.add_timer(2) do
        threads = []
        begin
          conn = node.global_connection(@db_instance)
          (conn.async?).should be_true
          db_num = conn.query("select count(*) as num from pg_database;").first['num'];
          end_time = start_time = Time.now.to_f
          # other threads in the process won't be blocked by database queries
          threads << Thread.new do
            cal = 0
            TEST_TIME.times do
              sleep 1
              cal += 1
            end
            end_time = Time.now.to_f
          end
          NUM.times do
            threads << Thread.new do
              loop do
                my_db_num = conn.query("select count(*) as num from pg_database;").first['num']
                db_num.should == my_db_num
                Thread.exit if  Time.now.to_f - start_time > TEST_TIME
              end
            end
          end
          NUM.times do
            threads << Thread.new do
              loop do
                my_db_num = 0
                conn.transaction do |c|
                  my_db_num = c.query("select count(*) as num from pg_database;").first['num']
                end
                db_num.should == my_db_num
                Thread.exit if  Time.now.to_f - start_time > TEST_TIME
              end
            end
          end
          threads.each{|t| t.join}
          (end_time - start_time).should >= 5
          (end_time - start_time).should < 6
        ensure
          VCAP::Services::Postgresql::Util::PGDBconn.use_async_query = ori_use_async_query
          EM.stop
        end
      end
    end
  end

  it "should handle discarded connections correctly" do
    EM.run do
      ori_use_async_query = VCAP::Services::Postgresql::Util::PGDBconn.use_async_query
      opts = @opts.dup
      opts[:db_use_async_query] = true
      opts[:not_start_instances] = true if opts[:use_warden]
      node = VCAP::Services::Postgresql::Node.new(opts)
      EM.add_timer(2) do
        begin
          conn = node.global_connection(@db_instance)
          # make sure the connections are active
          expect {conn.query('select current_timestamp')}.should_not raise_error
          conn.async?.should == true
          t1 = Thread.new do
            begin
              conn.query("select pg_sleep(2)")
            rescue
              nil
            end
          end
          node.add_discarded_connection(@db['name'], conn)
          node.add_discarded_connection('fake_name', nil)
          node.discarded_connections[@db['name']].size.should == 1
          node.discarded_connections['fake_name'].size.should == 1
          expect { node.discarded_connections[@db['name']].first.query("select current_timestamp") }.should_not raise_error
          node.postgresql_keep_alive
          node.discarded_connections[@db['name']].nil?.should == true
          node.discarded_connections['fake_name'].nil?.should == true
          node.postgresql_keep_alive
        ensure
          t1.join
          VCAP::Services::Postgresql::Util::PGDBconn.use_async_query = ori_use_async_query
          EM.stop
        end
      end
    end
  end

  it "should restore from backup file" do
    EM.run do
      tmp_db = @node.provision(@default_plan, nil, @default_version)
      tmp_instance = @node.pgProvisionedService.get(tmp_db['name'])
      @test_dbs[tmp_db] = []
      conn = connect_to_postgresql(tmp_db)
      old_db_info = @node.get_db_info(conn, tmp_db["name"])
      conn.query("create table test1(id int)")
      conn.query("insert into test1 values(1)")
      conn.query("create schema test_schema")
      conn.query("create table test_schema.test1(id int)")
      conn.query("insert into test_schema.test1 values(1)")
      postgresql_config = @node.postgresql_config(tmp_instance)
      host, port, user, password = %w(host port user pass).map{|key| postgresql_config[key]}
      tmp_file = "/tmp/#{tmp_db['name']}.dump"
      pg_dump = @opts[:postgresql][@default_version]['dump_bin']
      @node.dump_database(tmp_db['name'], host, port, tmp_db['user'], tmp_db['password'], tmp_file, :dump_bin => pg_dump).should == true
      conn.query("drop table test1")
      conn.query("drop table test_schema.test1")
      res = conn.query("select tablename from pg_catalog.pg_tables where schemaname = 'public';")
      res.count.should == 0
      res = conn.query("select tablename from pg_catalog.pg_tables where schemaname = 'test_schema';")
      res.count.should == 0

      conn.query("create table test2(id int)")
      conn.query("create table test_schema.test2(id int)")
      @node.restore(tmp_db["name"], "/tmp").should == true
      conn = connect_to_postgresql(tmp_db)
      new_db_info = @node.get_db_info(conn, tmp_db["name"])
      new_db_info["datconnlimit"].should == old_db_info["datconnlimit"]
      res = conn.query("select tablename from pg_catalog.pg_tables where schemaname = 'public';")
      res.count.should == 1
      res[0]["tablename"].should == "test1"
      res = conn.query("select tablename from pg_catalog.pg_tables where schemaname = 'test_schema';")
      res.count.should == 1
      res[0]["tablename"].should == "test1"
      res = conn.query("select id from test1")
      res.count.should == 1
      res = conn.query("select id from test_schema.test1")
      res.count.should == 1
      expect{ conn.query("create schema test_schmea2") }.should_not raise_error
      expect { conn.query("create temporary table temp_data as select * from test_schema.test1") }.should_not raise_error

      FileUtils.rm_rf(tmp_file)
      EM.stop
    end
  end

  it "should be able to get public schema id and get all user created schemas" do
    EM.run do
      node_public_schema_id =  @node.get_public_schema_id(@node.global_connection(@db_instance))
      node_public_schema_id.should_not  == nil

      tmp_db = @node.provision(@default_plan, nil, @default_version)
      tmp_instance = @node.pgProvisionedService.get(tmp_db['name'])
      @test_dbs[tmp_db] = []
      conn = connect_to_postgresql(tmp_db)
      default_user_public_schema_id = @node.get_public_schema_id(conn)
      default_user_public_schema_id.should == node_public_schema_id

      binding = @node.bind(tmp_db["name"], @default_opts)
      @test_dbs[tmp_db] << binding
      conn2 = connect_to_postgresql(binding)
      normal_user_public_schema_id = @node.get_public_schema_id(conn2)
      normal_user_public_schema_id.should == node_public_schema_id

      conn2.query("create schema test_schema1")
      conn2.query("create schema test_schema2")

      schemas = @node.get_conn_schemas(conn)
      schemas.size.should == 2
      schemas['test_schema1'].should_not == nil
      schemas['test_schema2'].should_not == nil

      conn.close if conn
      conn2.close if conn2

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
      expect { conn.query('select 1') }.should raise_error  # expected exception: connection terminated
      expect { conn2.query('select 1') }.should raise_error # expected exception: connection terminated
      expect { connect_to_postgresql(@db) }.should_not raise_error # default user won't be blocked
      expect { connect_to_postgresql(bind_cred) }.should raise_error #expected exception: no permission to connect
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
      db = @node.provision(@default_plan, nil, @default_version)
      @test_dbs[db] = []
      @node.dump_instance(db, [], '/tmp')
      @node.unprovision(db['name'], [])
      sleep 1 if @opts[:use_warden]
      @node.import_instance(db, {}, '/tmp', @default_plan).should == true
      conn = connect_to_postgresql(db)
      expect { conn.query('select 1') }.should_not raise_error
      EM.stop
    end
  end

  it "should recreate bindings when update instance handles" do
    EM.run do
      db = @node.provision(@default_plan, nil, @default_version)
      @test_dbs[db] = []
      binding = @node.bind(db['name'], @default_opts)
      @test_dbs[db] << binding
      conn = connect_to_postgresql(binding)
      value = {
        "fake_service_id" => {
          "credentials" => binding,
          "binding_options" => @default_opts,
        }
      }
      result = @node.update_instance(db, value).should be_true
      result.should be_instance_of Array
      expect { conn = connect_to_postgresql(binding) }.should_not raise_error
      expect { conn = connect_to_postgresql(db) }.should_not raise_error
      EM.stop
    end
  end

  it "should recreate bindings when enable instance" do
    EM.run do
      db = @node.provision(@default_plan, nil, @default_version)
      @test_dbs[db] = []
      binding = @node.bind(db['name'], @default_opts)
      @test_dbs[db] << binding
      conn = connect_to_postgresql(binding)
      @node.disable_instance(db, [binding])
      expect { conn = connect_to_postgresql(binding) }.should raise_error # expected exception: no permission to connect
      expect { conn = connect_to_postgresql(db) }.should_not raise_error
      value = {
        "fake_service_id" => {
          "credentials" => binding,
          "binding_options" => @default_opts,
        }
      }
      @node.enable_instance(db, value).should be_true
      expect { conn = connect_to_postgresql(binding) }.should_not raise_error
      expect { conn = connect_to_postgresql(db) }.should_not raise_error
      EM.stop
    end
  end

  it "should provision a database with correct credential" do
    EM.run do
      @db.should be_instance_of Hash
      conn = connect_to_postgresql(@db)
      expect { conn.query("SELECT 1") }.should_not raise_error
      conn.close if conn
      EM.stop
    end
  end

  it "should prevent user from altering db property" do
    EM.run do
      conn = connect_to_postgresql(@db)
      expect { conn.query("alter database #{@db["name"] } WITH CONNECTION LIMIT 1000")}.should raise_error(PGError, /must be owner of database .*/)
      conn.close if conn
      EM.stop
    end
  end

  it "should return correct instances & bindings list" do
    EM.run do
      before_ins_list = @node.all_instances_list
      tmp_db = @node.provision(@default_plan, nil, @default_version)
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
      tmp_db = @node.provision(@default_plan, nil, @default_version)
      @test_dbs[tmp_db] = []
      ins_list = @node.all_instances_list
      tmp_bind = @node.bind(tmp_db["name"], @default_opts)
      bind_list = @node.all_bindings_list
      oi = ins_list.find { |ins| ins == tmp_db["name"] }
      ob = bind_list.find { |bind| bind["name"] == tmp_bind["name"] and bind["username"] == tmp_bind["username"] }
      oi.should_not be_nil
      ob.should_not be_nil
      expect { @node.unbind(ob) }.should_not raise_error
      expect { @node.unprovision(oi, []) }.should_not raise_error
      # remove it from test_dbs for it is unprovisioned
      @test_dbs.delete(tmp_db)
      EM.stop
    end
  end

  it "should calculate both table and index as database size" do
    EM.run do
      conn = connect_to_postgresql(@db)
      ori_size = @node.db_size(@db_instance)
      # should calculate table size
      conn.query("CREATE TABLE test(id INT)")
      conn.query("INSERT INTO test VALUES(10)")
      conn.query("INSERT INTO test VALUES(20)")
      table_size = @node.db_size(@db_instance)
      table_size.should > ori_size
      # should also calculate index size
      conn.query("CREATE INDEX id_index on test(id)")
      all_size = @node.db_size(@db_instance)
      all_size.should > table_size
      conn.close if conn
      EM.stop
    end

  end

  it "should not create db or send response if receive a malformed request" do
    pending "Use warden, won't run this case." if @opts[:use_warden]
    EM.run do
      conn = @node.fetch_global_connection(@default_version)
      db_num = conn.query("select count(*) from pg_database;")[0]['count']
      mal_plan = "not-a-plan"
      db= nil
      expect {
        db=@node.provision(mal_plan, nil, @default_version)
        @test_dbs[db] = []
      }.should raise_error(VCAP::Services::Postgresql::PostgresqlError, /Invalid plan .*/)
      db.should == nil
      db_num.should == conn.query("select count(*) from pg_database;")[0]['count']
      EM.stop
    end
  end

  it "should not allow old credential to connect if service is unprovisioned" do
    EM.run do
      conn = connect_to_postgresql(@db)
      expect { conn.query("SELECT 1") }.should_not raise_error
      conn.close if conn
      msg = Yajl::Encoder.encode(@db)
      @node.unprovision(@db["name"], [])
      expect { connect_to_postgresql(@db) }.should raise_error
      # remove it from test_dbs for it is unprovisioned
      @test_dbs.delete(@db)
      EM.stop
    end
  end

  it "should clean up if service is unprovisioned" do
    class << @node
      attr_reader :free_ports
    end if @opts[:use_warden]

    EM.run do
      free_ports_size = @node.free_ports.size if @opts[:use_warden]
      db = @node.provision(@default_plan, nil, @default_version)
      @test_dbs[db] = []
      binding = @node.bind(db['name'], @default_opts)
      @test_dbs[db] << binding
      db_instance = @node.pgProvisionedService.get(db['name'])
      db_instance.should_not == nil
      version = db_instance.version
      @node.connections.include?(db['name']).should == true
      if @opts[:use_warden]
        @node.pgBindUser
             .all(:wardenprovisionedservice_name => db['name'])
             .count.should_not == 0
        @node.free_ports.include?(db["port"]).should_not == true
        @node.free_ports.size.should == (free_ports_size - 1)
      else
        @node.connections.include?(version).should == true
        @node.pgBindUser
             .all(:provisionedservice_name => db['name'])
             .count.should_not == 0
      end

      @node.unprovision(db["name"], [])
      @node.pgProvisionedService.get(db['name']).should ==  nil
      @node.connections.include?(db['name']).should == false
      if @opts[:use_warden]
        @node.pgBindUser
             .all(:wardenprovisionedservice_name => db['name'])
             .count.should == 0
        @node.free_ports.include?(db["port"]).should == true
        @node.free_ports.size.should == free_ports_size
      else
        @node.connections.include?(version).should == true
        @node.pgBindUser
             .all(:provisionedservice_name => db['name'])
             .count.should == 0
      end
      # remove it from test_dbs for it is unprovisioned
      @test_dbs.delete(db)
      EM.stop
    end
  end

  it "should return proper error if unprovision a not existing instance" do
    EM.run do
      expect {
        @node.unprovision("not-existing", [])
      }.should raise_error(VCAP::Services::Postgresql::PostgresqlError, /Postgresql configuration .* not found/)
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
      }.should raise_error(VCAP::Services::Postgresql::PostgresqlError,/Postgresql configuration .*not found/)

      # no existing credential
      credential = @node.bind(@db["name"],  @default_opts)
      credential.should_not == nil
      @test_dbs[@db] << credential

      # nil input
      @node.unbind(nil).should == nil
      EM.stop
    end
  end

  it "should prevent accessing database with wrong credentials" do
    EM.run do
      plan = "free"
      db2= @node.provision(plan, nil, @default_version)
      @test_dbs[db2] = []
      fake_creds = []
      # the case to login using wrong password is discarded for it will always fail (succeed to login without any exception): rules in pg_hba.conf will make this happen
      2.times {fake_creds << @db.clone}
      # try to login other's db
      fake_creds[0]["name"] = db2["name"]
      # try to login using the default account (parent role) of other's db default account
      fake_creds[1]["user"] = db2["user"]
      fake_creds.each do |creds|
        expect{ connect_to_postgresql(creds) }.should raise_error
      end
      EM.stop
    end
  end

  it "should kill long transaction" do
    EM.run do
      # reduce max_long_tx to accelerate test
      opts = @opts.dup
      opts[:max_long_tx] = 2
      opts[:not_start_instances] = true if opts[:use_warden]
      node = VCAP::Services::Postgresql::Node.new(opts)
      EM.add_timer(1.1) do
        binding = node.bind(@db['name'], @default_opts)
        @test_dbs[@db] << binding

        # use a superuser, won't be killed
        super_conn = node.management_connection(@db_instance, true)
        # prepare a transaction and not commit
        super_conn.query("create table a(id int)")
        super_conn.query("insert into a values(10)")
        super_conn.query("begin")
        super_conn.query("select * from a for update")
        EM.add_timer(opts[:max_long_tx] * 2) {
          expect do
            super_conn.query("select * from a for update")
            super_conn.query("commit")
          end.should_not raise_error
          super_conn.close if super_conn
        }

        # use a default user (parent role), won't be killed
        default_user = VCAP::Services::Postgresql::Node.pgProvisionedServiceClass(opts[:use_warden])
                        .get(@db['name'])
                        .pgbindusers
                        .all(:default_user => true)[0]
        user = @db.dup
        user['user'] = default_user[:user]
        user['password'] = default_user[:password]
        default_user_conn = connect_to_postgresql(user)
        # prepare a transaction and not commit
        default_user_conn.query("create table b(id int)")
        default_user_conn.query("insert into b values(10)")
        default_user_conn.query("begin")
        default_user_conn.query("select * from b for update")
        EM.add_timer(opts[:max_long_tx] * 2) {
          expect do
            default_user_conn.query("select * from b for update")
            default_user_conn.query("commit")
          end.should_not raise_error
          default_user_conn.close if default_user_conn
        }

        # use a non-default user (not parent role), will be killed
        user = @db.dup
        user['user'] = binding['user']
        user['password'] = binding['password']
        bind_conn = connect_to_postgresql(user)
        # prepare a transaction and not commit
        bind_conn.query("create table c(id int)")
        bind_conn.query("insert into c values(10)")
        bind_conn.query("begin")
        bind_conn.query("select * from c for update")
        EM.add_timer(opts[:max_long_tx] * 3) {
          expect { bind_conn.query("select * from c for update") }.should raise_error
          bind_conn.close if bind_conn
          EM.stop
        }
      end
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
      expect { conn.query("Select 1") }.should_not raise_error
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
      expect { conn = connect_to_postgresql(binding) }.should_not raise_error
      res = @node.unbind(binding)
      res.should be true
      expect { connect_to_postgresql(binding) }.should raise_error
      # old session should be killed
      expect { conn.query("SELECT 1") }.should raise_error
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
      bindings.each { |binding| expect { connect_to_postgresql(binding) }.should raise_error }
      # remove it from test_dbs for it is unprovisioned
      @test_dbs.delete(@db)
      EM.stop
    end
  end

  it "should able to generate varz" do
    EM.run do
      varz = @node.varz_details
      varz.should be_instance_of Hash
      varz[:db_stat].should be_instance_of Array
      varz[:max_capacity].should > 0
      varz[:available_capacity].should >= 0
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
      db = @node.provision(@default_plan, nil, @default_version)
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
      conn = @node.global_connection(@db_instance)
      if @opts[:use_warden]
        instance[:xlog_num].should == @node.xlog_file_num(@db_instance.base_dir)
      else
        instance[:xlog_num].should == @node.xlog_file_num(conn.settings['data_directory'])
      end
      EM.stop
    end
  end

  it "should report instance status in varz" do
    EM.run do
      varz = @node.varz_details()
      instance = @db['name']
      varz[:instances].each do |name, value|
        if (name == instance.to_sym)
          value.should == "ok"
        end
      end
      conn = @node.global_connection(@db_instance)
      conn.query("drop database #{instance}")
      varz = @node.varz_details()
      varz[:instances].each do |name, value|
        if (name == instance.to_sym)
          value.should == "fail"
        end
      end
      # restore db so cleanup code doesn't complain.
      conn.query("create database #{instance}")
      EM.stop
    end
  end

  it "should be thread safe" do
    # this case will consume 20 container space in the ci environment, with the current
    # ci bind mount scheme, this will be disk consuming, thus pending this case in ci env
    pending "thread safe case is disabled in ci environment due to resource issue" if ENV["CI_ENV"]
    EM.run do
      available_storage = @node.available_storage
      provision_served = @node.provision_served
      binding_served = @node.binding_served
      # Async query could increase the concurrency, so use smaller number here
      NUM = VCAP::Services::Postgresql::Util::PGDBconn.async? && @opts[:use_warden] ? 5 : 20
      threads = []
      NUM.times do
        threads << Thread.new do
          db = @node.provision(@default_plan, nil, @default_version)
          binding = @node.bind(db["name"], @default_opts)
          @test_dbs[db] = [binding]
          @node.unprovision(db["name"], [binding])
          @test_dbs.delete(db)
        end
      end
      threads.each {|t| t.join}
      available_storage.should == @node.available_storage
      provision_served.should == @node.provision_served - NUM
      binding_served.should == @node.binding_served - NUM
      EM.stop
    end
  end

  it "should evict page cache of loopback image files" do
    pending "You don't use warden or filesystem_quota, won't run this case." unless @opts[:use_warden] && @opts[:filesystem_quota]
    node = nil
    EM.run do
      opts = @opts.dup
      opts[:clean_image_cache] = true
      opts[:image_dir] = "/tmp/vcap_pg_pagecache_clean_test_dir"
      opts[:clean_image_cache_follow_interval] = 3
      File.init_fadvise_files

      # create fake image dir
      FileUtils.rm_rf(opts[:image_dir])
      FileUtils.rm_rf("#{opts[:image_dir]}_link")
      FileUtils.mkdir_p(opts[:image_dir])
      FileUtils.mkdir_p("#{opts[:image_dir]}_link")
      FileUtils.mkdir_p(File.join(opts[:image_dir], "subdir"))

      # linked files
      linked_file = File.join("#{opts[:image_dir]}_link", "linked_file")
      File.open(linked_file, 'w') do |f|
        f.write('linkedfile')
      end
      File.symlink(linked_file, File.join(opts[:image_dir], "link_file"))

      # regular files
      5.times do |i|
        reg_file = File.join(opts[:image_dir], i.to_s)
        File.open(reg_file, 'w') do |f|
          f.write('regular_file')
        end
      end

      # regular files in the subdirectory
      subdir_file = File.join(opts[:image_dir], "subdir", "subfile")
      File.open(subdir_file, 'w') do |f|
        File.open(subdir_file, 'w') do |f|
          f.write('subfile')
        end
      end
      opts[:not_start_instances] = true if @opts[:use_warden]
      node = VCAP::Services::Postgresql::Node.new(opts)
      EM.add_timer(3.5) do
        node.should_not == nil
        File.fadvise_files.count.should == 6
        EM.stop
      end
    end
  end

  it "should enforce xlog file number (periodical timer)" do
    pending "you should enable xlog_enforcer to test" unless @opts[:enable_xlog_enforcer]
    EM.run do
      opts = @opts.dup
      opts[:enable_xlog_enforcer] = true
      opts[:xlog_enforce_tolerance] = 1
      opts[:not_start_instances] = true if opts[:use_warden]
      node = VCAP::Services::Postgresql::Node.new(opts)

      EM.add_timer(1) do

        db_instance = node.pgProvisionedService.get(@db['name'])
        conn = node.global_connection(db_instance)
        expect { conn.query("select 1") }.should_not raise_error
        binding = node.bind(@db['name'], @default_opts)
        @test_dbs[@db] << binding
        binding_conn = connect_to_postgresql(binding)
        expect { binding_conn.query("select 1") }.should_not raise_error

        # use a default user (parent role), won't be killed
        default_user = VCAP::Services::Postgresql::Node.pgProvisionedServiceClass(opts[:use_warden])
                        .get(@db['name'])
                        .pgbindusers
                        .all(:default_user => true)[0]
        user = @db.dup
        user['user'] = default_user[:user]
        user['password'] = default_user[:password]
        default_user_conn = connect_to_postgresql(user)
        expect { default_user_conn.query("select 1") }.should_not raise_error

        data_dir = Dir.mktmpdir("xlog_test", "/tmp")
        ori_settings = conn.settings
        alert_times = 0
        begin
          # we won't count the files in real pg_xlog
          node.stub(:xlog_file_num) do |_|
            Dir.glob(File.join(data_dir, 'pg_xlog', '*')).select { |f| File.file?(f) }.count
          end
          conn.stub(:settings) do
            new_settings = ori_settings.dup
            new_settings["data_directory"] = data_dir
            new_settings["checkpoint_segments"] = 3
            new_settings
          end
          node.stub(:xlog_enforce_internal) do |_, arg_opts|
           alert_times += 1 if arg_opts[:alert_only]
          end unless opts[:use_warden]

          pg_xlog_dir = File.join(data_dir, "pg_xlog")
          Dir.mkdir(pg_xlog_dir)

          node.xlog_file_checkpoint_limit(conn).should == 11 # 3 * 3 + 2
          node.xlog_file_kill_limit(conn).should == node.xlog_file_checkpoint_limit(conn) + 3 # 4 * 3 + 2

          3.times { |i| FileUtils.touch(File.join(pg_xlog_dir, "00000-#{i}")) }

          node.xlog_file_num(data_dir).should == 3
          node.xlog_status(conn, data_dir).should == VCAP::Services::Postgresql::Node::XLOG_STATUS_OK

          EM.add_timer(1.1) do
            expect { binding_conn.query("select 1") }.should_not raise_error
            expect { default_user_conn.query("select 1") }.should_not raise_error
            expect { conn.query("select 1") }.should_not raise_error
            conn.checkpoint_times.nil?.should == true
            alert_times.should == 0
            9.times { |i| FileUtils.touch(File.join(pg_xlog_dir, "00001-#{i}")) }

            node.xlog_file_num(data_dir).should == 12
            node.xlog_status(conn, data_dir).should == VCAP::Services::Postgresql::Node::XLOG_STATUS_CHK

            EM.add_timer(1.1) do
              if opts[:use_warden]
                expect { binding_conn.query("select 1") }.should_not raise_error
                expect { default_user_conn.query("select 1") }.should_not raise_error
                expect { conn.query("select 1") }.should_not raise_error
                conn.checkpoint_times.should >= 1
              else
                expect { binding_conn.query("select 1") }.should_not raise_error
                expect { default_user_conn.query("select 1") }.should_not raise_error
                expect { conn.query("select 1") }.should_not raise_error
                conn.checkpoint_times.nil?.should == true
                alert_times.should >= 1
              end
              3.times { |i| FileUtils.touch(File.join(pg_xlog_dir, "00002-#{i}")) }

              node.xlog_status(conn, data_dir).should == VCAP::Services::Postgresql::Node::XLOG_STATUS_KILL

              EM.add_timer(2.2) do
                if opts[:use_warden]
                  expect { binding_conn.query("select 1") }.should raise_error
                  expect { default_user_conn.query("select 1") }.should_not raise_error
                  expect { conn.query("select 1") }.should_not raise_error
                  conn.checkpoint_times.should >= 3
                else
                  expect { binding_conn.query("select 1") }.should_not raise_error
                  expect { default_user_conn.query("select 1") }.should_not raise_error
                  expect { conn.query("select 1") }.should_not raise_error
                  conn.checkpoint_times.nil?.should == true
                  alert_times.should >= 3
                end
                EM.stop
              end
            end
         end
        ensure
          FileUtils.rm_f(data_dir)
        end
      end
    end
  end

  it "should enforce xlog file number (helper functions)" do
    EM.run do
      conn = @node.global_connection(@db_instance)
      binding = @node.bind(@db['name'], @default_opts)
      @test_dbs[@db] << binding
      binding_conn = connect_to_postgresql(binding)
      expect { binding_conn.query("select 1") }.should_not raise_error

      # use a default user (parent role), won't be killed
      default_user = VCAP::Services::Postgresql::Node.pgProvisionedServiceClass(@opts[:use_warden])
                      .get(@db['name'])
                      .pgbindusers
                      .all(:default_user => true)[0]
      user = @db.dup
      user['user'] = default_user[:user]
      user['password'] = default_user[:password]
      default_user_conn = connect_to_postgresql(user)
      expect { default_user_conn.query("select 1") }.should_not raise_error

      chk_segs = conn.settings['checkpoint_segments'].to_i
      @node.xlog_file_checkpoint_limit(conn).should == chk_segs * 3 + 2
      @node.xlog_file_kill_limit(conn).should ==  @node.xlog_file_checkpoint_limit(conn) + chk_segs
      Dir.mktmpdir("xlog_test", "/tmp") do |data_dir|
        pg_xlog_dir = File.join(data_dir, "pg_xlog")
        Dir.mkdir(pg_xlog_dir)
        Dir.mkdir(File.join(pg_xlog_dir, "xyz"))
        Tempfile.new("00000", File.join(pg_xlog_dir, "xyz"))
        # won't count directory and files in it
        @node.xlog_file_num(data_dir).should == 0
        chk_segs.times do
          Tempfile.new("00000", pg_xlog_dir)
        end
        @node.xlog_file_num(data_dir).should == chk_segs
        @node.xlog_status(conn, data_dir).should == VCAP::Services::Postgresql::Node::XLOG_STATUS_OK

        (chk_segs * 2 + 3).times do
          Tempfile.new("00000", pg_xlog_dir)
        end
        @node.xlog_status(conn, data_dir).should == VCAP::Services::Postgresql::Node::XLOG_STATUS_CHK

        chk_segs.times do
          Tempfile.new("00000", pg_xlog_dir)
        end
        @node.xlog_status(conn, data_dir).should == VCAP::Services::Postgresql::Node::XLOG_STATUS_KILL

        @node.xlog_enforce_internal(conn, :alert_only => true)
        expect { binding_conn.query("select 1") }.should_not raise_error
        expect { default_user_conn.query("select 1") }.should_not raise_error
        expect { conn.query("select 1") }.should_not raise_error
        conn.checkpoint_times.nil?.should == true

        @node.xlog_enforce_internal(
          conn,
          :xlog_status => VCAP::Services::Postgresql::Node::XLOG_STATUS_CHK)
        expect { binding_conn.query("select 1") }.should_not raise_error
        expect { default_user_conn.query("select 1") }.should_not raise_error
        expect { conn.query("select 1") }.should_not raise_error
        conn.checkpoint_times.should == 1

        @node.xlog_enforce_internal(
          conn,
          :excluded_users => [default_user[:user]],
          :xlog_status => VCAP::Services::Postgresql::Node::XLOG_STATUS_KILL)
        expect { binding_conn.query("select 1") }.should raise_error
        expect { default_user_conn.query("select 1") }.should_not raise_error
        expect { conn.query("select 1") }.should_not raise_error
        conn.checkpoint_times.should == 2
        EM.stop
      end
    end
  end

  it "should enforce database size quota" do
    node = nil
    EM.run do
      opts = @opts.dup
      # reduce storage quota
      opts[:not_start_instances] = true if @opts[:use_warden]
      # add extra 0.5MB(524288B) to the size of a new intialized instance to calculate max_db_size
      # so inserting 1MB(1000000B) data must trigger the quota enforcement
      # sleep enough time to get the steady size value
      sleep 1
      opts[:max_db_size] = (@node.db_size(@db_instance) + 524288)/1024.0/1024.0
      node = VCAP::Services::Postgresql::Node.new(opts)
      EM.add_timer(1.1) do
        node.should_not == nil
        binding = node.bind(@db['name'], @default_opts)
        @test_dbs[@db] << binding
        EM.add_timer(2) do
          conn = connect_to_postgresql(binding)
          conn.query("create table test(data text)")
          conn.query("create schema quota_schema")
          conn.query("create table quota_schema.test(data text)")
          conn.query("insert into quota_schema.test values('test_quota')")
          c =  [('a'..'z'),('A'..'Z')].map{|i| Array(i)}.flatten
          # prepare 1M data
          content = (0..1000000).map{ c[rand(c.size)] }.join
          conn.query("create temporary table temp_table (data text) on commit delete rows")
          conn.query("insert into test values('#{content}')")
          EM.add_timer(2) do
            # terminating connection due to administrator command
            expect { conn.query("select version()") }.should raise_error(PGError)
            conn.close if conn
            first_conn = connect_to_postgresql(binding)
            expect { first_conn.query("select version()") }.should_not raise_error
            second_binding = node.bind(@db['name'], @default_opts)
            second_conn = connect_to_postgresql(second_binding)
            [first_conn, second_conn].each do |conn|
              # write permission denied for relation test
              expect { conn.query("select * from test limit 1") }.should_not raise_error(PGError)
              expect { conn.query("insert into test values('1')") }.should raise_error(PGError)
              expect { conn.query("create table test1(data text)") }.should raise_error(PGError)
              expect { conn.query("select * from quota_schema.test limit 1") }.should_not raise_error(PGError)
              expect { conn.query("insert into quota_schema.test values('2')") }.should raise_error(PGError)
              expect { conn.query("create table quota_schema.test1(data text)") }.should raise_error(PGError)
              expect { conn.query("create schema new_quota_schema") }.should raise_error(PGError)

              # temp permission denied
              expect { conn.query("create temporary table test2 (data text) on commit delete rows") }.should raise_error(PGError)
              expect { conn.query("drop temporary table temp_table") }.should raise_error(PGError)
            end

            first_conn.query("truncate test") # delete from won't reduce the db size immediately
            EM.add_timer(2) do
              # write privilege should be restored
              expect { first_conn.query("insert into test values('1')") }.should_not raise_error
              expect { first_conn.query("create table test1(data text)") }.should_not raise_error
              expect { first_conn.query("insert into quota_schema.test values(1)")}.should_not raise_error
              expect { first_conn.query("create table quota_schema.test1(data text)") }.should_not raise_error
              expect { first_conn.query("create schema new_quota_schema") }.should_not raise_error
              # temp privilege should be restored
              expect { first_conn.query("create temporary table test2 (data text) on commit delete rows") }.should_not raise_error
              expect { first_conn.query("drop temporary table temp_table") }.should raise_error
              first_conn.close if first_conn
              second_conn.close if second_conn
              EM.stop
            end
          end
        end
      end
    end
  end

  it "should survive checking quota of a non-existent instance" do
    EM.run do
      # this test verifies that we've fixed a race condition between
      # the quota-checker and unprovision/unbind
      db = @node.provision(@default_plan, nil, @default_version)
      @test_dbs[db] = []
      service = @node.get_service(db)
      service.should be
      @node.unprovision(db['name'], [])
      @test_dbs.delete(db)
      # we can now simulate the quota-enforcer checking an
      # unprovisioned instance
      expect { @node.revoke_write_access(service) }.should_not raise_error
      expect { @node.grant_write_access(service) }.should_not raise_error
      # actually, the bug was not that these methods raised
      # exceptions, but rather that they called Kernel.exit.  so the
      # real proof that we've fixed the bug is that this test finishes
      # at all...
      EM.stop
    end
  end

  it "should be able to share objects across users" do
    EM.run do
      user1 = @node.bind @db["name"], @default_opts
      conn1 = connect_to_postgresql user1
      conn1.query 'create table t_user1(i int)'
      conn1.query 'create sequence s_user1'
      conn1.query "create function f_user1() returns integer as 'select 1234;' language sql"
      conn1.close if conn1

      user2 = @node.bind @db["name"], @default_opts
      conn2 = connect_to_postgresql user2
      expect { conn2.query 'drop table t_user1' }.should_not raise_error
      expect { conn2.query 'drop sequence s_user1' }.should_not raise_error
      expect { conn2.query 'drop function f_user1()' }.should_not raise_error
      conn2.close if conn2
      EM.stop
    end
  end

  it "should keep all objects created by a user after the user deleted, then new user is able to access those objects" do
    EM.run do
      user = @node.bind @db["name"], @default_opts
      conn = connect_to_postgresql user
      conn.query 'create table t(i int)'
      conn.query 'create sequence s'
      conn.query "create function f() returns integer as 'select 1234;' language sql"
      conn.close if conn
      @node.unbind user

      user = @node.bind @db["name"], @default_opts
      conn = connect_to_postgresql user
      expect { conn.query 'drop table t' }.should_not raise_error
      expect { conn.query 'drop sequence s' }.should_not raise_error
      expect { conn.query 'drop function f()' }.should_not raise_error
      conn.close if conn
      EM.stop
    end
  end

  it "should work that user2 can bring the db back to normal after user1 puts much data to cause quota enforced" do
    node = nil
    EM.run do
      opts = @opts.dup
      # reduce storage quota.
      if opts[:use_warden]
         opts[:not_start_instances] = true
      end
      # add extra 0.5MB(524288B) to the size of a new intialized instance to calculate max_db_size
      # so inserting 1MB(1000000B) data must trigger the quota enforcement
      sleep 1
      opts[:max_db_size] = (@node.db_size(@db_instance) + 524288)/1024.0/1024.0

      node = VCAP::Services::Postgresql::Node.new(opts)
      EM.add_timer(1.1) do
        node.should_not == nil
        binding = node.bind(@db['name'], @default_opts)
        @test_dbs[@db] << binding
        EM.add_timer(2) do
          conn = connect_to_postgresql(binding)
          conn.query("create table test(data text)")
          conn.query("create schema new_schema")
          conn.query("create table new_schema.test(data text)")
          conn.query("insert into new_schema.test values('1')")
          c =  [('a'..'z'),('A'..'Z')].map{|i| Array(i)}.flatten
          # prepare 1M data
          content = (0..1000000).map{ c[rand(c.size)] }.join
          conn.query("insert into test values('#{content}')")
          EM.add_timer(2) do
            # terminating connection due to administrator command
            expect { conn.query("select version()") }.should raise_error(PGError)
            conn.close if conn
            conn = connect_to_postgresql(binding)
            expect { conn.query("select version()") }.should_not raise_error(PGError)
            # permission denied for relation test
            expect { conn.query("insert into test values('1')") }.should raise_error(PGError)
            expect { conn.query("create table test1(data text)") }.should raise_error(PGError)
            expect { conn.query("insert into new_schema.test values('1')") }.should raise_error(PGError)
            expect { conn.query("create schema another_schema") }.should raise_error(PGError)
            # user2 deletes data
            binding_2 = node.bind(@db['name'], @default_opts)
            conn2 = connect_to_postgresql(binding_2)
            conn2.query("truncate test")
            EM.add_timer(2) do
              # write privilege should be restored
              expect { conn.query("insert into test values('1')") }.should_not raise_error
              expect { conn.query("create table test1(data text)") }.should_not raise_error
              expect { conn.query("insert into new_schema.test values('1')") }.should_not raise_error
              expect { conn.query("create schema another_schema") }.should_not raise_error
              expect { conn2.query("insert into test values('1')") }.should_not raise_error
              expect { conn2.query("create table test2(data text)") }.should_not raise_error
              expect { conn2.query("insert into new_schema.test values('1')") }.should_not raise_error
              expect { conn2.query("create schema another_schema2") }.should_not raise_error
              conn.close if conn
              conn2.close if conn2
              EM.stop
            end
          end
        end
      end
    end
  end

  after:each do
    @node.class.setup_datamapper(:default, @opts[:local_db])
    EM.run do
      @test_dbs.keys.each do |db|
        begin
          name = db["name"]
          @node.unprovision(name, @test_dbs[db])
          @node.logger.info("Clean up database: #{name}")
        rescue => e
          @node.logger.error("Error during cleanup database: #{e} - #{e.backtrace.join('|')}")
        end
      end
      EM.add_timer(0.1) {EM.stop}
    end unless @test_dbs.empty?
    if @opts[:use_warden]
      FileUtils.rm_rf "/tmp/vcap_pg_pagecache_clean_test_dir"
      FileUtils.rm_rf "/tmp/vcap_pg_pagecache_clean_test_dir_link"
      # reset back class vars if you changed
      VCAP::Services::Postgresql::Node.pgProvisionedServiceClass(true).init(@opts)
    end
    @node.class.setup_datamapper(:default, @opts[:local_db])
  end

  after:all do
    FileUtils.rm_f Dir.glob('/tmp/d*.dump')
  end
end


describe "Postgresql node special cases" do
  include VCAP::Services::Postgresql
  before :all do
    @opts = getNodeTestConfig
    @default_plan = "free"
    @default_version = @opts[:default_version]
    @default_opts = "default"
  end

  it "should limit max connection to the database" do
    node = nil
    opts = getNodeTestConfig
    EM.run do
      opts[:max_db_conns] = 1
      node = VCAP::Services::Postgresql::Node.new(opts)
      EM.add_timer(2) {EM.stop}
    end
    db = node.provision(@default_plan, nil, @default_version)
    conn = connect_to_postgresql(db)
    expect { conn.query("SELECT 1") }.should_not raise_error
    expect { connect_to_postgresql(db) }.should raise_error(PGError, /too many connections for database .*/)
    conn.close if conn
    node.unprovision(db["name"], [])
  end

  it "should handle postgresql error in varz" do
    node = nil
    opts = getNodeTestConfig
    pending "Use warden, won't run this case." if opts[:use_warden]
    EM.run do
      node = VCAP::Services::Postgresql::Node.new(opts)
      EM.add_timer(2) {EM.stop}
    end
    # drop connection
    node.fetch_global_connection(@default_version).close
    varz = nil
    expect { varz = node.varz_details }.should_not raise_error
    #varz.should == {}
  end

  it "should return node not ready if postgresql server is not connected" do
    node = nil
    opts = getNodeTestConfig
    pending "Use warden, won't run this case." if opts[:use_warden]
    EM.run do
      node = VCAP::Services::Postgresql::Node.new(opts)
      EM.add_timer(2) {EM.stop}
    end
    node.fetch_global_connection(@default_version).close
    # keep_alive interval is 15 seconds so it should be ok
    node.connection_exception(
      node.fetch_global_connection(@default_version)
    ).should be_instance_of PGError
    node.node_ready?.should == false
    node.send_node_announcement.should == nil
  end

  it "should keep alive" do
    node = nil
    opts = getNodeTestConfig
    EM.run do
      node = VCAP::Services::Postgresql::Node.new(opts)
      EM.add_timer(0.1) {EM.stop}
    end
    db = node.provision(@default_plan, nil, @default_version)
    db_instance = node.pgProvisionedService.get(db['name'])
    node.get_status(db_instance).should == 'ok'
    expect { node.global_connection(db_instance).query("select current_timestamp") }.should_not raise_error
    node.global_connection(db_instance).close
    node.get_status(db_instance).should == 'ok'
    expect { node.global_connection(db_instance).query("select current_timestamp") }.should raise_error
    node.postgresql_keep_alive
    node.get_status(db_instance).should == 'ok'
    expect {node.global_connection(db_instance).query("select current_timestamp") }.should_not raise_error
  end
end
