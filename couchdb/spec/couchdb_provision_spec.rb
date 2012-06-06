# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require "spec_helper"

describe "couchdb_node provision" do

  before :all do
    @opts = get_node_config()
    @couchdb_config = @opts[:couchdb]
    delete_leftover_users
  end

  before :each do
    EM.run do
      @logger = @opts[:logger]
      @node = Node.new(@opts)
      @original_memory = @node.available_memory

      @resp = @node.provision("free")
      EM.stop
    end
  end

  after :each do
    @node.unprovision(@resp['name'], []) if @resp
    delete_leftover_users
  end

  it "should have valid response" do
    @resp.should_not be_nil
    inst_name = @resp['name']
    inst_name.should_not be_nil
    inst_name.should_not == ""
  end

  it "should be able to connect to couchdb" do
    is_port_open?('127.0.0.1', @resp['port']).should be_true
  end

  it "should not allow unauthorized user to access the instance" do
    EM.run do
      begin
        conn = CouchRest.new("http://#{@resp['host']}:#{@resp['port']}")
        coll = conn.database(@resp['name'])
        coll.save_doc({'a' => 1})
      rescue Exception => e
        @logger.debug e
      end
      e.to_s.should == %{401 Unauthorized: {"error":"unauthorized","reason":"Authentication required."}\n}
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
      stats[:running_services][0]['overall']['couchdb'].should_not be_nil
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
      conn = server_connection(@resp)
      coll = conn.database(@resp['name'])
      coll.save_doc({'a' => 1})
      coll.all_docs["total_rows"].should == 1
      EM.stop
    end
  end

  it "should return error when unprovisioning a non-existed instance" do
    EM.run do
      e = nil
      begin
        @node.unprovision('no existed', [])
      rescue => e
      end
      e.message.should == "Error Code: 30300, Error Message: no existed not found"
      EM.stop
    end
  end

  it "should report error when admin users are deleted from couchdb" do
    EM.run do
      delete_admin(@opts[:couchdb], @resp)
      stats = @node.varz_details
      stats.should_not be_nil
      stats[:running_services].length.should > 0
      stats[:running_services][0]['db'].class.should == String
      stats[:running_services][0]['overall'].class.should == String
      EM.stop
    end
  end

  it "should be able to unprovision an existing instance" do
    EM.run do
      resp = @resp
      @node.unprovision(@resp['name'], [])
      @resp = nil
      EM.stop
    end
  end

  it "should not be able to access after unprovision" do
    EM.run do
      resp = @resp
      @node.unprovision(@resp['name'], [])
      @resp = nil

      e = nil
      begin
        conn = server_connection(resp)
        coll = conn.database(resp['name'])
        coll.save_doc({'a' => 1})
      rescue => e
      end
      e.to_s.should == %{401 Unauthorized: {"error":"unauthorized","reason":"Name or password is incorrect."}\n}
      EM.stop
    end
  end

  it "should have no provisioned instances after unprovision" do
    EM.run do
      @node.unprovision(@resp['name'], [])
      @resp = nil

      VCAP::Services::CouchDB::Node::ProvisionedService.count.should == 0
      EM.stop
    end
  end

  it "should have no users after unprovision" do
    EM.run do
      @node.unprovision(@resp['name'], [])
      @resp = nil

      conn = server_admin_connection
      db = conn.database("_users")
      db.all_docs["rows"].select { |u| u["id"] =~ /^org.couchdb.user:/ }.should == []
      EM.stop
    end
  end

  it "should consume node's memory" do
    EM.run do
      free_plan_memory = VCAP::Services::CouchDB::Node::ProvisionedService.last.memory

      (@original_memory - @node.available_memory).should == free_plan_memory
      s1 = @node.provision("free")
      (@original_memory - @node.available_memory).should == 2 * free_plan_memory
      s2 = @node.provision("free")
      (@original_memory - @node.available_memory).should == 3 * free_plan_memory

      @node.unprovision(s2['name'], [])
      @node.unprovision(s1['name'], [])
      EM.stop
    end
  end

  it "should calcuate node's memory when instanced" do
    EM.run do
      free_plan_memory = VCAP::Services::CouchDB::Node::ProvisionedService.last.memory

      s1 = @node.provision("free")
      s2 = @node.provision("free")

      node = Node.new(@opts)
      (@opts[:available_memory] - node.available_memory).should == 3 * free_plan_memory

      @node.unprovision(s2['name'], [])
      @node.unprovision(s1['name'], [])
      EM.stop
    end
  end

  it "should release memory" do
    EM.run do
      @node.unprovision(@resp['name'], [])
      @resp = nil

      @original_memory.should == @node.available_memory
      EM.stop
    end
  end
end
