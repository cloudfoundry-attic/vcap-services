# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require "spec_helper"

describe "mongodb backup/restore"  do

  before :all do
    @app_id = "myapp"
    @opts = get_node_config()
    @logger = @opts[:logger]

    @opts[:mongod_path].match "(.+#{File::SEPARATOR}).+"
    BINARY_DIR = $1

    @config_template = ERB.new(File.read(TEMPLATE_FILE))
    config = @config_template.result(binding)
    FileUtils.rm_f(CONFIG_FILE)
    File.open(CONFIG_FILE, "w") {|f| f.write(config)}
    FileUtils.rm_rf(BACKUP_DIR)

    EM.run do
      @node = Node.new(@opts)
      EM.add_timer(3) { EM.stop }
    end
  end

  before :each do
    @resp = @node.provision("free")
    sleep 1
    @bind_resp = @node.bind(@resp['name'], 'rw')
    # Write some data in database
    conn = Mongo::Connection.new(@bind_resp['hostname'], @bind_resp['port']).db(@resp['db'])
    conn.authenticate(@bind_resp['username'], @bind_resp['password'])
    coll = conn.collection(TEST_COLL)
    row = { TEST_KEY => TEST_VAL }
    coll.insert(row)
  end

  after :each do
    @node.unbind(@bind_resp).should be_true
    EM.run do
      @node.unprovision(@resp['name'], [])
      EM.add_timer(3) { EM.stop }
    end
    e = nil
    begin
      conn = Mongo::Connection.new('localhost', @resp['port']).db('db')
    rescue => e
    end
    e.should_not be_nil
  end

  after :all do
    FileUtils.rm_f(CONFIG_FILE)
    FileUtils.rm_rf(BACKUP_DIR)
  end

  it "should be able to backup the database" do
    # Run mongodb_backup
    res = Kernel.system("../bin/mongodb_backup -c #{CONFIG_FILE} -t > /dev/null")
    res.should == true
  end

  it "should be able to restore the database" do
    # Run mongodb_backup
    res = Kernel.system("../bin/mongodb_backup -c #{CONFIG_FILE} -t > /dev/null")
    raise 'mongodb_backup failed' unless res

    # Change value after backup
    conn = Mongo::Connection.new(@bind_resp['hostname'], @bind_resp['port']).db(@resp['db'])
    conn.authenticate(@bind_resp['username'], @bind_resp['password'])
    coll = conn.collection(TEST_COLL)
    row = { TEST_KEY => TEST_VAL }
    coll.update(row, { TEST_KEY => TEST_VAL_2 })

    # Get backup file location
    dir = get_backup_dir(BACKUP_DIR)

    # Run restore
    @node.restore(@resp['name'], dir)

    # wait for restore to happen
    sleep 10

    # Should be the same like what it was before backup
    doc = coll.find_one()
    doc[TEST_KEY].should == TEST_VAL
  end

  it "should be able to recover the backup instance" do
    # Run mongodb_backup
    res = Kernel.system("../bin/mongodb_backup -c #{CONFIG_FILE} -t > /dev/null")
    raise 'mongodb_backup failed' unless res

    # Get backup file location
    dir = get_backup_dir(BACKUP_DIR)

    @node.unbind(@bind_resp)
    EM.run do
      @node.unprovision(@resp['name'], [])
      EM.add_timer(5) { EM.stop }
    end
    @node.provision('free', @resp)
    @node.restore(@resp['name'], dir)
    @node.bind(@resp['name'], 'rw', @bind_resp)

    # Should be the same like what it was before backup
    conn = Mongo::Connection.new(@bind_resp['hostname'], @bind_resp['port']).db(@resp['db'])
    conn.authenticate(@bind_resp['username'], @bind_resp['password'])
    coll = conn.collection(TEST_COLL)
    doc = coll.find_one()
    doc[TEST_KEY].should == TEST_VAL
  end

end


