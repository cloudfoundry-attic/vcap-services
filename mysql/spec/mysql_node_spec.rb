# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require 'spec_helper'
require 'mysql_service/node'
require 'mysql_service/mysql_error'
require 'mysql2'
require 'yajl'
require 'fileutils'


module VCAP
  module Services
    module Mysql
      class Node
        attr_reader :pool, :logger, :capacity, :provision_served, :binding_served
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
    @opts.freeze
    # Setup code must be wrapped in EM.run
    EM.run do
      @node = VCAP::Services::Mysql::Node.new(@opts)
      EM.add_timer(1) { EM.stop }
    end
    @tmpfiles = []
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

  it "should connect to mysql database" do
    EM.run do
      expect {@node.pool.with_connection{|connection| connection.query("SELECT 1")}}.should_not raise_error
      EM.stop
    end
  end

  it "should report inconsistency between mysql and local db" do
    EM.run do
      name, user = @db["name"], @db["user"]
      @node.pool.with_connection do |conn|
        conn.query("delete from db where db='#{name}' and user='#{user}'")
      end
      result = @node.check_db_consistency
      result.include?([name, user]).should == true
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

  it "should calculate both table and index as database size" do
    EM.run do
      conn = connect_to_mysql(@db)
      # should calculate table size
      conn.query("CREATE TABLE test(id INT)")
      conn.query("INSERT INTO test VALUE(10)")
      conn.query("INSERT INTO test VALUE(20)")
      table_size = @node.dbs_size(conn)[@db["name"]]
      table_size.should > 0
      # should also calculate index size
      conn.query("CREATE INDEX id_index on test(id)")
      all_size = @node.dbs_size(conn)[@db["name"]]
      all_size.should > table_size
      EM.stop
    end
  end

  it "should enforce database size quota" do
    EM.run do
      opts = @opts.dup
      # reduce storage quota to 20KB, default mysql allocates pages of 16kb, so inserting anything will allocate at least 16k
      opts[:max_db_size] = 20.0/1024
      node = VCAP::Services::Mysql::Node.new(opts)
      EM.add_timer(1) do
        binding = node.bind(@db["name"],  @default_opts)
        @test_dbs[@db] << binding
        conn = connect_to_mysql(binding)
        conn.query("create table test(data text)")
        c =  [('a'..'z'),('A'..'Z')].map{|i| Array(i)}.flatten
        content = (0..21000).map{ c[rand(c.size)] }.join   # enough data to exceed quota for sure
        conn.query("insert into test value('#{content}')")
        EM.add_timer(3) do
          expect {conn.query('SELECT 1')}.should raise_error
          conn.close
          conn = connect_to_mysql(binding)
          # write privilege should be rovoked.
          expect{ conn.query("insert into test value('test')")}.should raise_error(Mysql2::Error)
          conn = connect_to_mysql(@db)
          expect{ conn.query("insert into test value('test')")}.should raise_error(Mysql2::Error)
          # new binding's write privilege should also be revoked.
          new_binding = node.bind(@db['name'], @default_opts)
          @test_dbs[@db] << new_binding
          new_conn = connect_to_mysql(new_binding)
          expect { new_conn.query("insert into test value('new_test')")}.should raise_error(Mysql2::Error)
          EM.add_timer(3) do
            expect {conn.query('SELECT 1')}.should raise_error
            conn.close
            conn = connect_to_mysql(binding)
            conn.query("delete from test")
            # write privilege should restore
            EM.add_timer(5) do   # we need at least 5s for information_schema tables to update with new data_length
              conn = connect_to_mysql(binding)
              expect{ conn.query("insert into test value('test')")}.should_not raise_error
              conn.query("insert into test value('#{content}')")
              EM.add_timer(3) do
                expect { conn.query('SELECT 1') }.should raise_error
                conn.close
                conn = connect_to_mysql(binding)
                expect{ conn.query("insert into test value('test')") }.should raise_error(Mysql2::Error)
                conn.query("drop table test")
                EM.add_timer(2) do
                  conn = connect_to_mysql(binding)
                  expect { conn.query("create table test(data text)") }.should_not raise_error
                  expect { conn.query("insert into test value('test')") }.should_not raise_error
                  EM.stop
                end
              end
            end
          end
        end
      end
    end
  end

  it "should able to handle orphan instances when enforce storage quota." do
    begin
      # forge an orphan instance, which is not exist in mysql
      klass = VCAP::Services::Mysql::Node::ProvisionedService
      DataMapper.setup(:default, @opts[:local_db])
      DataMapper::auto_upgrade!
      service = klass.new
      service.name = 'test-'+ UUIDTools::UUID.random_create.to_s
      service.user = "test"
      service.password = "test"
      service.plan = 1
      if not service.save
        raise "Failed to forge orphan instance: #{service.errors.inspect}"
      end
      EM.run do
        expect { @node.enforce_storage_quota }.should_not raise_error
        EM.stop
      end
    ensure
      service.destroy
    end
  end

  it "should return correct instances & binding list" do
    EM.run do
      before_ins_list = @node.all_instances_list
      plan = "free"
      tmp_db = @node.provision(plan)
      @test_dbs[tmp_db] = []
      after_ins_list = @node.all_instances_list
      before_ins_list << tmp_db["name"]
      (before_ins_list.sort == after_ins_list.sort).should be_true

      before_bind_list = @node.all_bindings_list
      tmp_credential = @node.bind(tmp_db["name"],  @default_opts)
      @test_dbs[tmp_db] << tmp_credential
      after_bind_list = @node.all_bindings_list
      before_bind_list << tmp_credential
      a,b = [after_bind_list,before_bind_list].map do |list|
        list.map{|item| item["username"]}.sort
      end
      (a == b).should be_true

      EM.stop
    end
  end

  it "should not create db or send response if receive a malformed request" do
    EM.run do
      @node.pool.with_connection do |connection|
        db_num = connection.query("show databases;").count
        mal_plan = "not-a-plan"
        db = nil
        expect {
          db = @node.provision(mal_plan)
        }.should raise_error(VCAP::Services::Mysql::MysqlError, /Invalid plan .*/)
        db.should == nil
        db_num.should == connection.query("show databases;").count
      end
      EM.stop
    end
  end

  it "should support over provisioning" do
    EM.run do
      opts = @opts.dup
      opts[:capacity] = 10
      opts[:max_db_size] = 20
      node = VCAP::Services::Mysql::Node.new(opts)
      EM.add_timer(1) do
        expect {
          db = node.provision(@default_plan)
          @test_dbs[db] = []
        }.should_not raise_error
        EM.stop
      end
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
      }.should raise_error(VCAP::Services::Mysql::MysqlError, /Mysql configuration .* not found/)
      # nil input handle
      @node.unprovision(nil, []).should == nil
      EM.stop
    end
  end

  it "should not be possible to access one database using null or wrong credential" do
    EM.run do
      plan = "free"
      db2 = @node.provision(plan)
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
    if @opts[:max_long_tx] > 0 and (@node.check_innodb_plugin)
      EM.run do
        opts = @opts.dup
        # reduce max_long_tx to accelerate test
        opts[:max_long_tx] = 1
        node = VCAP::Services::Mysql::Node.new(opts)
        EM.add_timer(1) do
          conn = connect_to_mysql(@db)
          # prepare a transaction and not commit
          conn.query("create table a(id int) engine=innodb")
          conn.query("insert into a value(10)")
          conn.query("begin")
          conn.query("select * from a for update")
          old_killed = node.varz_details[:long_transactions_killed]
          EM.add_timer(opts[:max_long_tx] * 5) {
            expect {conn.query("select * from a for update")}.should raise_error(Mysql2::Error)
            conn.close
            node.varz_details[:long_transactions_killed].should > old_killed

            node.instance_variable_set(:@kill_long_tx, false)
            conn = connect_to_mysql(@db)
            # prepare a transaction and not commit
            conn.query("begin")
            conn.query("select * from a for update")
            old_counter = node.varz_details[:long_transactions_count]
            EM.add_timer(opts[:max_long_tx] * 5) {
              expect {conn.query("select * from a for update")}.should_not raise_error(Mysql2::Error)
              node.varz_details[:long_transactions_count].should > old_counter
              old_counter = node.varz_details[:long_transactions_count]
              EM.add_timer(opts[:max_long_tx] * 5) {
                #counter should not double-count the same long transaction
                node.varz_details[:long_transactions_count].should == old_counter
                conn.close
                EM.stop
              }
            }
          }
        end
      end
    else
      pending "long transaction killer is disabled."
    end
  end

  it "should kill long queries" do
    pending "Disable for non-Percona server since the test behavior varies on regular Mysql server." unless @node.is_percona_server?
    EM.run do
      db = @node.provision(@default_plan)
      @test_dbs[db] = []
      opts = @opts.dup
      opts[:max_long_query] = 1
      conn = connect_to_mysql(db)
      node = VCAP::Services::Mysql::Node.new(opts)
      EM.add_timer(1) do
        conn.query('create table test(id INT) engine innodb')
        conn.query('insert into test value(10)')
        conn.query('begin')
        # lock table test
        conn.query('select * from test where id = 10 for update')
        old_counter = node.varz_details[:long_queries_killed]

        conn2 = connect_to_mysql(db)
        err = nil
        t = Thread.new do
          begin
            # conn2 is blocked by conn, we use lock to simulate long queries
            conn2.query("select * from test for update")
          rescue => e
            err = e
          ensure
            conn2.close
          end
        end

        EM.add_timer(opts[:max_long_query] * 5){
          err.should_not == nil
          err.message.should =~ /interrupted/
            # counter should also be updated
            node.varz_details[:long_queries_killed].should > old_counter
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
      expect {connect_to_mysql(binding)}.should raise_error
      # old session should be killed
      expect {conn.query("SELECT 1")}.should raise_error(Mysql2::Error)
      EM.stop
    end
  end

  it "should not delete user in credential when unbind 'ancient' instances" do
    EM.run do
      # Crafting an ancient binding credential which is the same as provision credential
      ancient_binding = @db.dup
      expect { connect_to_mysql(ancient_binding) }.should_not raise_error
      @node.unbind(ancient_binding)
      # ancient_binding is still valid after unbind
      expect { connect_to_mysql(ancient_binding) }.should_not raise_error
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

  it "should able to restore database from backup file" do
    EM.run do
      db = @node.provision(@default_plan)
      @test_dbs[db] = []
      conn = connect_to_mysql(db)
      conn.query("create table test(id INT)")
      conn.query("create procedure defaultfunc(out defaultcount int) begin select count(*) into defaultcount from test; end")
      binding = @node.bind(db['name'], @default_opts)
      new_binding = @node.bind(db['name'], @default_opts)
      @test_dbs[db] << binding
      @test_dbs[db] << new_binding

      # create stored procedure
      bind_conn = connect_to_mysql(binding)
      new_bind_conn = connect_to_mysql(new_binding)
      bind_conn.query("create procedure myfunc(out mycount int) begin  select count(*) into mycount from test ; end")
      bind_conn.query("create procedure myfunc2(out mycount int) SQL SECURITY invoker begin select count(*) into mycount from test;end")
      new_bind_conn.query("create procedure myfunc3(out mycount int) begin select count(*) into mycount from test; end")
      new_bind_conn.close if new_bind_conn
      @node.unbind(new_binding)
      conn.query("call defaultfunc(@testcount)")
      conn.query("select @testcount")
      conn.query("call myfunc(@testcount)")
      conn.query("select @testcount")
      conn.query("call myfunc2(@testcount)")
      conn.query("select @testcount")
      conn.query("call myfunc3(@testcount)")
      conn.query("select @testcount")
      bind_conn.query("call defaultfunc(@testcount)")
      bind_conn.query("select @testcount")
      bind_conn.query("call myfunc(@testcount)")
      bind_conn.query("select @testcount")
      bind_conn.query("call myfunc2(@testcount)")
      bind_conn.query("select @testcount")
      bind_conn.query("call myfunc3(@testcount)")
      bind_conn.query("select @testcount")


      # backup current db
      host, port, user, password = %w(host port user pass).map{|key| @opts[:mysql][key]}
      tmp_file = "/tmp/#{db['name']}.sql.gz"
      @tmpfiles << tmp_file
      result = `mysqldump -h #{host} -P #{port} --user='#{user}' --password='#{password}' -R #{db['name']} | gzip > #{tmp_file}`
      bind_conn.query("drop procedure myfunc")
      conn.query("drop table test")
      res = bind_conn.query("show procedure status")
      res.count().should == 3
      res = conn.query("show tables")
      res.count.should == 0

      # create a new table which should be deleted after restore
      conn.query("create table test2(id int)")
      bind_conn.close if bind_conn
      conn.close if conn
      @node.unbind(binding)
      @node.restore(db["name"], "/tmp/").should == true
      conn = connect_to_mysql(db)
      res = conn.query("show tables")
      res.count().should == 1
      res.first["Tables_in_#{db['name']}"].should == "test"
      res = conn.query("show procedure status")
      res.count().should == 4
      expect do
        conn.query("call defaultfunc(@testcount)")
        conn.query("select @testcount")
      end.should_not raise_error
      expect do
        conn.query("call myfunc(@testcount)")
        conn.query("select @testcount")
      end.should_not raise_error # secuirty type should be invoker or a error will be raised.
      expect do
        conn.query("call myfunc2(@testcount)")
        conn.query("select @testcount")
      end.should_not raise_error
      expect do
        conn.query("call myfunc3(@testcount)")
        conn.query("select @testcount")
      end.should_not raise_error
      EM.stop
    end
  end

  it "should be able to disable an instance" do
    EM.run do
      bind_cred = @node.bind(@db["name"],  @default_opts)
      conn = connect_to_mysql(bind_cred)
      @test_dbs[@db] << bind_cred
      @node.disable_instance(@db, [bind_cred])
      # kill existing session
      expect { conn.query('SELECT 1')}.should raise_error
      expect { conn2.query('SELECT 1')}.should raise_error
      # delete user
      expect { connect_to_mysql(bind_cred)}.should raise_error
      EM.stop
    end
  end

  it "should able to dump instance content to file" do
    EM.run do
      conn = connect_to_mysql(@db)
      conn.query('create table MyTestTable(id int)')
      @node.dump_instance(@db, nil, '/tmp').should == true
      File.open(File.join("/tmp", "#{@db['name']}.sql")) do |f|
        line = f.each_line.find {|line| line =~ /MyTestTable/}
        line.should_not be nil
      end
      @tmpfiles << File.join("/tmp", "#{@db['name']}.sql")
      EM.stop
    end
  end

  it "should recreate database and user when import instance" do
    EM.run do
      db = @node.provision(@default_plan)
      @test_dbs[db] = []
      @node.dump_instance(db, nil , '/tmp')
      @node.unprovision(db['name'], [])
      @node.import_instance(db, {}, '/tmp', @default_plan).should == true
      conn = connect_to_mysql(db)
      expect { conn.query('SELECT 1')}.should_not raise_error
      @tmpfiles << File.join("/tmp", "#{db['name']}.sql")
      EM.stop
    end
  end

  it "should recreate bindings when enable instance" do
    EM.run do
      db = @node.provision(@default_plan)
      @test_dbs[db] = []
      binding = @node.bind(db['name'], @default_opts)
      @test_dbs[db] << binding
      conn = connect_to_mysql(binding)
      @node.disable_instance(db, [binding])
      expect {conn = connect_to_mysql(binding)}.should raise_error
      value = {
        "fake_service_id" => {
          "credentials" => binding,
          "binding_options" => @default_opts,
        }
      }
      @node.enable_instance(db, value).should be_true
      expect {conn = connect_to_mysql(binding)}.should_not raise_error
      EM.stop
    end
  end

  it "should recreate bindings when update instance handles" do
    EM.run do
      db = @node.provision(@default_plan)
      @test_dbs[db] = []
      binding = @node.bind(db['name'], @default_opts)
      @test_dbs[db] << binding
      conn = connect_to_mysql(binding)
      @node.disable_instance(db, [binding])
      expect {conn = connect_to_mysql(binding)}.should raise_error
      value = {
        "fake_service_id" => {
          "credentials" => binding,
          "binding_options" => @default_opts,
        }
      }
      result = @node.update_instance(db, value)
      result.should be_instance_of Array
      expect {conn = connect_to_mysql(binding)}.should_not raise_error
      EM.stop
    end
  end

  it "should retain instance data after node restart" do
    EM.run do
      node = VCAP::Services::Mysql::Node.new(@opts)
      EM.add_timer(1) do
        db = node.provision(@default_plan)
        @test_dbs[db] = []
        conn = connect_to_mysql(db)
        conn.query('create table test(id int)')
        # simulate we restart the node
        node.shutdown
        node = VCAP::Services::Mysql::Node.new(@opts)
        EM.add_timer(1) do
          conn2 = connect_to_mysql(db)
          result = conn2.query('show tables')
          result.count.should == 1
          EM.stop
        end
      end
    end
  end

  it "should able to generate varz." do
    EM.run do
      node = VCAP::Services::Mysql::Node.new(@opts)
      EM.add_timer(1) do
        varz = node.varz_details
        varz.should be_instance_of Hash
        varz[:queries_since_startup].should >0
        varz[:queries_per_second].should >= 0
        varz[:database_status].should be_instance_of Array
        varz[:max_capacity].should > 0
        varz[:available_capacity].should >= 0
        varz[:long_queries_killed].should >= 0
        varz[:long_transactions_killed].should >= 0
        varz[:provision_served].should >= 0
        varz[:binding_served].should >= 0
        EM.stop
      end
    end
  end

  it "should handle Mysql error in varz" do
    pending "This test is not capatiable with mysql2 conenction pool."
    EM.run do
      node = VCAP::Services::Mysql::Node.new(@opts)
      EM.add_timer(1) do
        # drop mysql connection
        node.pool.close
        varz = nil
        expect {varz = node.varz_details}.should_not raise_error
        varz.should == {}
        EM.stop
      end
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
      instance = v[:database_status].find {|d| d[:name] == @db["name"]}
      instance.should_not be_nil
      instance[:size].should >= 0
      EM.stop
    end
  end

  it "should report node instance status in varz" do
    pending "This test is not capatiable with mysql2 conenction pool."
    EM.run do
      varz = @node.varz_details
      varz[:instances].each do |name, status|
        status.shoud  == "ok"
      end
      node = VCAP::Services::Mysql::Node.new(@opts)
      EM.add_timer(1) do
        node.pool.close
        varz = node.varz_details
        varz[:instances].each do |name, status|
          status.should == "ok"
        end
        EM.stop
      end
    end
  end

  it "should report instance status in varz" do
    EM.run do
      varz = @node.varz_details()
      instance = @db['name']
      varz[:instances].each do |name, value|
        if name == instance.to_sym
          value.should == "ok"
        end
      end
      @node.pool.with_connection do |connection|
        connection.query("Drop database #{instance}")
        sleep 1
        varz = @node.varz_details()
        varz[:instances].each do |name, value|
          if name == instance.to_sym
            value.should == "fail"
          end
        end
        # restore db so cleanup code doesn't complain.
        connection.query("create database #{instance}")
      end
      EM.stop
    end
  end

  it "should be thread safe" do
    EM.run do
      provision_served = @node.provision_served
      binding_served = @node.binding_served
      # Set concurrent threads to pool size. Prevent pool is empty error.
      NUM = @node.pool.size
      threads = []
      NUM.times do
        threads << Thread.new do
          db = @node.provision(@default_plan)
          binding = @node.bind(db["name"], @default_opts)
          @node.unprovision(db["name"], [binding])
        end
      end
      threads.each {|t| t.join}
      provision_served.should == @node.provision_served - NUM
      binding_served.should == @node.binding_served - NUM
      EM.stop
    end
  end

  it "should enforce max connection limitation per user account" do
    EM.run do
      opts = @opts.dup
      opts[:max_user_conns] = 1 # easy for testing
      node = VCAP::Services::Mysql::Node.new(opts)
      EM.add_timer(1) do
        db = node.provision(@default_plan)
        binding = node.bind(db["name"],  @default_opts)
        @test_dbs[db] = [binding]
        expect { conn = connect_to_mysql(db) }.should_not raise_error
        expect { conn = connect_to_mysql(binding) }.should_not raise_error
        EM.stop
      end
    end
  end

  it "should add timeout option to all management mysql connection" do
    EM.run do
      opts = @opts.dup
      origin_timeout = Mysql2::Client.default_timeout
      timeout = 1
      opts[:connection_wait_timeout] = timeout
      node = VCAP::Services::Mysql::Node.new(opts)

      EM.add_timer(2) do
        begin
          # server side timeout
          node.pool.with_connection do |conn|
            # simulate connection idle
            sleep (timeout * 5)
            expect{ conn.query("select 1") }.should raise_error(Mysql2::Error, /MySQL server has gone away/)
          end
          # client side timeout
          node.pool.with_connection do |conn|
            # override server side timeout
            conn.query("set @@wait_timeout=10")
            expect{ conn.query("select sleep(5)") }.should raise_error(Timeout::Error)
          end
        ensure
          # restore original timeout
          Mysql2::Client.default_timeout = origin_timeout
          EM.stop
        end
      end
    end
  end

  it "should works well if timeout is disabled for management mysql connection" do
    EM.run do
      opts = @opts.dup
      origin_timeout = Mysql2::Client.default_timeout
      opts.delete :connection_wait_timeout
      node = VCAP::Services::Mysql::Node.new(opts)

      EM.add_timer(2) do
        begin
          # server side timeout
          node.pool.with_connection do |conn|
            sleep (5)
            expect{ conn.query("select 1") }.should_not raise_error
          end
          # client side timeout
          node.pool.with_connection do |conn|
            expect{ conn.query("select sleep(5)") }.should_not raise_error
          end
        ensure
          # restore original timeout
          Mysql2::Client.default_timeout = origin_timeout
          EM.stop
        end
      end
    end
  end

  after :each do
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

  after :all do
    @tmpfiles.each do |tmpfile|
      FileUtils.rm_r tmpfile
    end
  end
end
