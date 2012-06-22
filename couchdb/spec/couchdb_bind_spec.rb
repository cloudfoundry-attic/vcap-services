# Copyright (c) 2009-2011 VMware, Inc.
require "spec_helper"

describe "couchdb_node bind" do

  before :all do
    @opts = get_node_config()
    @couchdb_config = @opts[:couchdb]
    start_couchdb_server("#{@opts[:couchdb_install_path]}/etc/init.d/couchdb")
    delete_leftover_users
  end

  before :each do
    EM.run do
      @logger = @opts[:logger]
      @node = Node.new(@opts)
      @resp = @node.provision("free")

      @bind_resp = @node.bind(@resp['name'], 'rw')
      EM.stop
    end
  end

  after :each do
    @node.unbind(@bind_resp) if @bind_resp
    @node.unprovision(@resp['name'], []) if @resp
    delete_leftover_users
  end

  after :all do
    stop_couchdb_server("#{@opts[:couchdb_install_path]}/etc/init.d/couchdb")
  end

  it "should have valid response" do
    @resp.should_not be_nil
    @resp['hostname'].should_not be_nil
    @resp['hostname'].should == @bind_resp['host']
    @resp['port'].should_not be_nil
    @resp['username'].should_not be_nil
    @resp['password'].should_not be_nil
    @bind_resp.should_not be_nil
    @bind_resp['hostname'].should_not be_nil
    @bind_resp['hostname'].should == @bind_resp['host']
    @bind_resp['port'].should_not be_nil
    @bind_resp['username'].should_not be_nil
    @bind_resp['password'].should_not be_nil
  end

  it "should be able to connect to couchdb" do
    is_port_open?('127.0.0.1', @resp['port']).should be_true
  end

  it "should return error when tring to bind on non-existed instance" do
    e = nil
    begin
      @node.bind('non-existed', 'rw')
    rescue => e
    end
    e.class.should == VCAP::Services::Base::Error::ServiceError
    e.message.should == 'Error Code: 30300, Error Message: non-existed not found'
  end

  it "should allow authorized user to access the instance" do
    EM.run do
      conn = server_connection(@bind_resp)
      coll = conn.database(@bind_resp['name'])
      before = coll.documents["total_rows"]
      coll.save_doc({'a' => 1})
      (coll.documents["total_rows"] - before).should == 1
      EM.stop
    end
  end

  it "should not allow authorized user to access another instance" do
    EM.run do
      another = @node.provision("free")

      begin
        conn = server_connection(@bind_resp)
        coll = conn.database(another['name'])
        before = coll.documents["total_rows"]
      rescue => e
      end
      e.to_s.should == %{401 Unauthorized: {"error":"unauthorized","reason":"You are not authorized to access this db."}\n}

      @node.unprovision(another['name'], [])
      EM.stop
    end
  end

  it "should not allow unauthorized user to access the instance" do
    EM.run do
      conn = server_connection('host' => @resp['host'], 'port' => @resp['port'])
      coll = conn.database(@resp['name'])
      begin
        coll.save_doc({'a' => 1})
      rescue => e
      end
      e.to_s.should == %{401 Unauthorized: {"error":"unauthorized","reason":"You are not authorized to access this db."}\n}
      EM.stop
    end
  end

  it "should not allow valid user with empty password to access the instance" do
    EM.run do
      conn = server_connection(@bind_resp.merge('password' => nil))
      coll = conn.database(@bind_resp['name'])
      begin
        coll.save_doc({'a' => 1})
      rescue => e
      end
      e.to_s.should == %{401 Unauthorized: {"error":"unauthorized","reason":"Name or password is incorrect."}\n}
      EM.stop
    end
  end

  it "should return error when trying to unbind a non-existed service" do
    EM.run do
      begin
        resp  = @node.unbind('not existed')
      rescue => e
      end
      e.message.should == "Error Code: 30300, Error Message:  not found"
      EM.stop
    end
  end

  # unbind here
  it "should be able to unbind it" do
    EM.run do
      resp  = @node.unbind(@bind_resp)
      @bind_resp = nil

      resp.should be_true
      EM.stop
    end
  end

  it "should remove the bind user on unbind" do
    EM.run do
      @node.unbind(@bind_resp)
      @bind_resp = nil

      conn = server_admin_connection
      db = conn.database("_users")
      users = db.documents["rows"].select { |u| u["id"] =~ /^org.couchdb.user:/ }
      users.length.should == 1
      users[0]["id"].should == "org.couchdb.user:#{@resp['username']}"
      EM.stop
    end
  end

  it "should not allow user to access the instance after unbind" do
    EM.run do
      bind_resp = @bind_resp
      @node.unbind(@bind_resp)
      @bind_resp = nil

      begin
        conn = server_connection(bind_resp)
        coll = conn.database(bind_resp['name'])
        coll.save_doc({'a' => 1})
      rescue => e
        e.to_s.should == %{401 Unauthorized: {"error":"unauthorized","reason":"Name or password is incorrect."}\n}
      end
      EM.stop
    end
  end

  it "should not be able to use bind credentials to access an unprovisioned instance" do
    EM.run do
      @node.unprovision(@resp['name'], [])
      @resp = nil

      e = nil
      begin
        conn = server_connection(@bind_resp)
        coll = conn.database(@bind_resp['name'])
        coll.documents
      rescue => e
      end
      e.to_s.should == %{401 Unauthorized: {"error":"unauthorized","reason":"Name or password is incorrect."}\n}

      @bind_resp = nil
      EM.stop
    end
  end
end
