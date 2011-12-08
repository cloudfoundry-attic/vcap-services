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
        attr_reader :connection, :logger, :available_storage, :provision_served, :binding_served
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
      original = @node.available_storage
      db2 = @node.provision(@default_plan)
      @test_dbs[db2] = []
      current = @node.available_storage
      (original - current).should == @opts[:max_db_size]*1024*1024
      @node.unprovision(db2["name"],[])
      unprov = @node.available_storage
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
      table_size = @node.dbs_size[@db["name"]]
      table_size.should > 0
      # should also calculate index size
      conn.query("CREATE INDEX id_index on test(id)")
      all_size = @node.dbs_size[@db["name"]]
      all_size.should > table_size
      EM.stop
    end
  end

  it "should enforce database size quota" do
    EM.run do
      opts = @opts.dup
      # reduce storage quota to 4KB.
      opts[:max_db_size] = 4.0/1024
      node = VCAP::Services::Mysql::Node.new(opts)
      binding = node.bind(@db["name"],  @default_opts)
      @test_dbs[@db] << binding
      conn = connect_to_mysql(binding)
      conn.query("create table test(data text)")
      c =  [('a'..'z'),('A'..'Z')].map{|i| Array(i)}.flatten
      content = (0..5000).map{ c[rand(c.size)] }.join
      conn.query("insert into test value('#{content}')")
      EM.add_timer(3) do
        expect {conn.ping()}.should raise_error
        conn.close
        conn = connect_to_mysql(binding)
        # write privilege should be rovoked.
        expect{ conn.query("insert into test value('test')")}.should raise_error(Mysql::Error, /INSERT command denied/)
        conn2 = connect_to_mysql(@db)
        expect{ conn2.query("insert into test value('test')")}.should raise_error(Mysql::Error, /INSERT command denied/)
        conn.query("delete from test")
        EM.add_timer(3) do
          expect {conn.ping()}.should raise_error
          conn.close
          conn = connect_to_mysql(binding)
          # write privilege should restore
          expect{ conn.query("insert into test value('test')")}.should_not raise_error
          EM.stop
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
      service.plan = "free"
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
      db_num = @node.connection.query("show databases;").num_rows()
      mal_plan = "not-a-plan"
      db = nil
      expect {
        db = @node.provision(mal_plan)
      }.should raise_error(MysqlError, /Invalid plan .*/)
      db.should == nil
      db_num.should == @node.connection.query("show databases;").num_rows()
      EM.stop
    end
  end

  it "should support over provisioning" do
    EM.run do
      opts = @opts.dup
      opts[:available_storage] = 10
      opts[:max_db_size] = 20
      node = VCAP::Services::Mysql::Node.new(opts)
      expect {
        node.provision(@default_plan)
      }.should_not raise_error
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
        conn = connect_to_mysql(@db)
        # prepare a transaction and not commit
        conn.query("create table a(id int) engine=innodb")
        conn.query("insert into a value(10)")
        conn.query("begin")
        conn.query("select * from a for update")
        EM.add_timer(opts[:max_long_tx] * 5) {
          expect {conn.query("select * from a for update")}.should raise_error(Mysql::Error, /interrupted/)
          conn.close
          EM.stop
        }
      end
    else
      pending "long transaction killer is disabled."
    end
  end

  it "should kill long queries" do
    EM.run do
      db = @node.provision(@default_plan)
      @test_dbs[db] = []
      opts = @opts.dup
      opts[:max_long_query] = 1
      conn = connect_to_mysql(db)
      node = VCAP::Services::Mysql::Node.new(opts)
      conn.query('create table test(id INT) engine innodb')
      conn.query('insert into test value(10)')
      conn.query('begin')
      # lock table test
      conn.query('select * from test where id = 10 for update')
      err = nil
      old_counter = node.varz_details[:long_queries_killed]
      t = Proc.new do
        EM.add_timer(opts[:max_long_query] * 2){
          err.should_not == nil
          err.should =~ /interrupted/
          # counter should also be updated
          node.varz_details[:long_queries_killed].should > old_counter
          EM.stop
        }
        begin
          name, host, port, user, pass = %w(name hostname port user password).map{|key| db[key]}
          # use deadlock to simulate long queries
          err = %x[echo 'select * from test for update'|#{@opts[:mysql_bin]} -h #{host} -P #{port} -u #{user} --password=#{pass} #{name} 2>&1]
        rescue => e
          err = e
        end
        conn.close
      end
      EM.defer(t)
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
      expect {connect_to_mysql(binding_res)}.should raise_error
      # old session should be killed
      expect {conn.query("SELECT 1")}.should raise_error(Mysql::Error, /MySQL server has gone away/)
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
      # backup current db
      host, port, user, password = %w(host port user pass).map{|key| @opts[:mysql][key]}
      tmp_file = "/tmp/#{db['name']}.sql.gz"
      result = `mysqldump -h #{host} -P #{port} -u #{user} --password=#{password} #{db['name']} | gzip > #{tmp_file}`
      conn.query("drop table test")
      res = conn.query("show tables")
      res.num_rows().should == 0
      # create a new table which should be deleted after restore
      conn.query("create table test2(id int)")
      @node.restore(db["name"], "/tmp/").should == true
      conn = connect_to_mysql(db)
      res = conn.query("show tables")
      res.num_rows().should == 1
      res.fetch_row[0].should == "test"
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
      expect { conn.query('select 1')}.should raise_error
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
      EM.stop
    end
  end

  it "should recreate database and user when import instance" do
    EM.run do
      db = @node.provision(@default_plan)
      @test_dbs[db] = []
      @node.dump_instance(db, nil , '/tmp')
      @node.unprovision(db['name'], [])
      @node.import_instance(db, [], '/tmp', @default_plan).should == true
      conn = connect_to_mysql(db)
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
      conn = connect_to_mysql(binding)
      @node.disable_instance(db, [binding])
      expect {conn = connect_to_mysql(binding)}.should raise_error
      value = {
        "fake_service_id" => {
          "credentials" => binding,
          "binding_options" => @default_opts,
        }
      }
      result = @node.enable_instance(db, value)
      result.should be_instance_of Array
      expect {conn = connect_to_mysql(binding)}.should_not raise_error
      EM.stop
    end
  end

  it "should retain instance data after node restart" do
    EM.run do
      node = VCAP::Services::Mysql::Node.new(@opts)
      db = node.provision(@default_plan)
      @test_dbs[db] = []
      conn = connect_to_mysql(db)
      conn.query('create table test(id int)')
      # simulate we restart the node
      node.shutdown
      node = VCAP::Services::Mysql::Node.new(@opts)
      conn2 = connect_to_mysql(db)
      result = conn2.query('show tables')
      result.num_rows().should == 1
      EM.stop
    end
  end

  it "should able to generate varz." do
    EM.run do
      varz = @node.varz_details
      varz.should be_instance_of Hash
      varz[:queries_since_startup].should >0
      varz[:queries_per_second].should >= 0
      varz[:database_status].should be_instance_of Array
      varz[:node_storage_capacity].should > 0
      varz[:node_storage_used].should >= 0
      varz[:long_queries_killed].should >= 0
      varz[:long_transactions_killed].should >= 0
      varz[:provision_served].should >= 0
      varz[:binding_served].should >= 0
      EM.stop
    end
  end

  it "should handle Mysql error in varz" do
    EM.run do
      node = VCAP::Services::Mysql::Node.new(@opts)
      # drop mysql connection
      node.connection.close
      varz = nil
      expect {varz = node.varz_details}.should_not raise_error
      varz.should == {}
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
      instance = v[:database_status].find {|d| d[:name] == @db["name"]}
      instance.should_not be_nil
      instance[:size].should >= 0
      EM.stop
    end
  end

  it "should update node capacity after provision new instance" do
    EM.run do
      v1 = @node.varz_details
      db = @node.provision(@default_plan)
      @test_dbs[db] =[]
      v2 = @node.varz_details
      (v2[:node_storage_used] - v1[:node_storage_used]).should ==
        (@opts[:max_db_size] * 1024 * 1024)
      EM.stop
    end
  end

  it "should report node status in healthz" do
    EM.run do
      healthz = @node.healthz_details()
      healthz[:self].should == "ok"
      node = VCAP::Services::Mysql::Node.new(@opts)
      node.connection.close
      healthz = node.healthz_details()
      healthz[:self].should == "fail"
      EM.stop
    end
  end

  it "should close extra mysql connections after generate healthz" do
    EM.run do
      res = @node.connection.list_processes
      conns_before_healthz = res.num_rows
      healthz = @node.healthz_details()
      healthz.keys.size.should >= 2
      res = @node.connection.list_processes
      conns_after_healthz =  res.num_rows
      conns_before_healthz.should == conns_after_healthz
      EM.stop
    end
  end

  it "should report instance status in healthz" do
    EM.run do
      healthz = @node.healthz_details()
      instance = @db['name']
      healthz[instance.to_sym].should == "ok"
      conn = @node.connection
      conn.query("Drop database #{instance}")
      healthz = @node.healthz_details()
      healthz[instance.to_sym].should == "fail"
      # restore db so cleanup code doesn't complain.
      conn.query("create database #{instance}")
      EM.stop
    end
  end

  it "should be thread safe" do
    EM.run do
      available_storage = @node.available_storage
      provision_served = @node.provision_served
      binding_served = @node.binding_served
      NUM = 20
      threads = []
      NUM.times do
        threads << Thread.new do
          db = @node.provision(@default_plan)
          binding = @node.bind(db["name"], @default_opts)
          @node.unprovision(db["name"], [binding])
        end
      end
      threads.each {|t| t.join}
      available_storage.should == @node.available_storage
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
      db = node.provision(@default_plan)
      binding = node.bind(db["name"],  @default_opts)
      @test_dbs[db] = [binding]
      expect { conn = connect_to_mysql(db) }.should_not raise_error
      expect { conn = connect_to_mysql(db) }.should raise_error(Mysql::Error, /exceeded the 'max_user_connections'/)
      expect { conn = connect_to_mysql(binding) }.should_not raise_error
      expect { conn = connect_to_mysql(binding) }.should raise_error(Mysql::Error, /exceeded the 'max_user_connections'/)
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
