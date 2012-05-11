# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require "spec_helper"

describe "MongoDB node backup/restore"  do

  before :all do
    @opts = get_node_config()
    @logger = @opts[:logger]
    DB_URL = @opts[:local_db]

    bin_dir = File.dirname(@opts[:mongod_path])
    BINARY_DIR = bin_dir == '.' ? '' : bin_dir
    MONGOD_LOG = @opts[:mongod_log_dir]

    @config_template = ERB.new(File.read(TEMPLATE_FILE))
    config = @config_template.result(binding)
    FileUtils.mkdir_p(File.dirname(CONFIG_FILE))
    FileUtils.rm_f(CONFIG_FILE)
    File.open(CONFIG_FILE, "w") {|f| f.write(config)}
    FileUtils.rm_rf(BACKUP_DIR)

    EM.run do
      @node = Node.new(@opts)
      EM.add_timer(1) { EM.stop }
    end
  end

  before :each do
    @resp = @node.provision("free")
    @bind_resp = @node.bind(@resp['name'], 'rw')
    @p_service = @node.get_instance(@resp['name'])
    # Write some data in database
    conn = Mongo::Connection.new(@p_service.ip, '27017')
    db = conn.db(@resp['db'])
    db.authenticate(@bind_resp['username'], @bind_resp['password'])
    coll = db.collection(TEST_COLL)
    row = { TEST_KEY => TEST_VAL }
    coll.insert(row)
  end

  after :each do
    @node.unbind(@bind_resp).should be_true
    @node.unprovision(@resp['name'], [])
  end

  after :all do
    @node.shutdown if @node
    FileUtils.rm_f(CONFIG_FILE)
    FileUtils.rm_rf(BACKUP_DIR)
  end

  it "should be able to backup/restore the database" do
    # Run mongodb_backup
    res = Kernel.system("../bin/mongodb_backup -c #{CONFIG_FILE} -t >> /tmp/mongodb_back.log")
    raise "mongodb_backup failed #{res}" unless res

    # Change value after backup
    conn = Mongo::Connection.new(@p_service.ip, '27017')
    db = conn.db(@resp['db'])
    db.authenticate(@bind_resp['username'], @bind_resp['password'])
    coll = db.collection(TEST_COLL)
    row = { TEST_KEY => TEST_VAL }
    coll.update(row, { TEST_KEY => TEST_VAL_2 })

    # Get backup file location
    dir = get_backup_dir(BACKUP_DIR)

    # Run restore
    @node.restore(@resp['name'], dir)

    # Should be the same like what it was before backup
    doc = coll.find_one()
    doc[TEST_KEY].should == TEST_VAL
  end

  it "should be able to recover the backup instance" do
    # Run mongodb_backup
    res = Kernel.system("../bin/mongodb_backup -c #{CONFIG_FILE} -t >> /tmp/mongodb_back.log")
    raise "mongodb_backup failed #{res}" unless res

    # Get backup file location
    dir = get_backup_dir(BACKUP_DIR)

    @node.unbind(@bind_resp)
    @node.unprovision(@resp['name'], [])

    @resp = @node.provision('free', @resp)
    @p_service = @node.get_instance(@resp['name'])
    @node.restore(@resp['name'], dir)
    @node.bind(@resp['name'], 'rw', @bind_resp)

    # Should be the same like what it was before backup
    conn = Mongo::Connection.new(@p_service.ip, '27017')
    db = conn.db(@resp['db'])
    db.authenticate(@bind_resp['username'], @bind_resp['password'])
    coll = db.collection(TEST_COLL)
    doc = coll.find_one()
    doc[TEST_KEY].should == TEST_VAL
  end

end
