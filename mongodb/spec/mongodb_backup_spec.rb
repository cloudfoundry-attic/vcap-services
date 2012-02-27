# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require "spec_helper"

describe "mongodb backup/restore"  do

  before :all do
    EM.run do
      @app_id = "myapp"
      @opts = get_node_config()
      @logger = @opts[:logger]

      BINARY_DIR = File.dirname(@opts[:mongod_path])
      @node = Node.new(@opts)
      @resp = @node.provision("free")

      @config_template = ERB.new(File.read(TEMPLATE_FILE))
      config = @config_template.result(binding)
      FileUtils.rm_f(CONFIG_FILE)
      File.open(CONFIG_FILE, "w") {|f| f.write(config)}

      FileUtils.rm_rf(BACKUP_DIR)

      EM.add_timer(1) do
        @bind_resp = @node.bind(@resp['name'], 'rw')
        EM.add_timer(1) do
          EM.stop
        end
      end
    end
  end

  after :all do
    FileUtils.rm_f(CONFIG_FILE)
    FileUtils.rm_rf(BACKUP_DIR)
  end

  it "should be able to backup the database" do
    # Write some data in database
    conn = Mongo::Connection.new(@bind_resp['hostname'], @bind_resp['port']).db(@resp['db'])
    conn.authenticate(@bind_resp['username'], @bind_resp['password'])
    coll = conn.collection(TEST_COLL)
    row = { TEST_KEY => TEST_VAL }
    coll.insert(row)

    # Run mongodb_backup
    res = Kernel.system("../bin/mongodb_backup -c #{CONFIG_FILE} -t > /dev/null")
    raise 'mongodb_backup failed' unless res

    # Change value after backup
    coll.update(row, { TEST_KEY => TEST_VAL_2 })
  end

  it "should be able to restore the database" do
    # Get backup file location
    dir = get_backup_dir(BACKUP_DIR)

    # Run restore
    @node.restore(@resp['name'], dir)

    # wait for restore to happen
    sleep 10

    # Should be the same like what it was before backup
    conn = Mongo::Connection.new(@bind_resp['hostname'], @bind_resp['port']).db(@resp['db'])
    conn.authenticate(@bind_resp['username'], @bind_resp['password'])
    coll = conn.collection(TEST_COLL)
    doc = coll.find_one()
    doc[TEST_KEY].should == TEST_VAL
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


