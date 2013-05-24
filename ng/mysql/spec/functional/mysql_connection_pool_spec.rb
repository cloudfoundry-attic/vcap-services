# Copyright (c) 2009-2011 VMware, Inc.
require 'spec_helper'
require 'logger'
require 'yajl'
require 'mysql_service/util'
require 'timeout'

module VCAP
  module Services
    module Mysql
      module Util
        class ConnectionPool
          attr_reader :connections, :shutting_down
          attr_accessor :max, :min
        end
      end
    end
  end
end

describe 'Mysql Connection Pool Test' do

  before :all do
    @opts = getNodeTestConfig
    @logger = @opts[:logger]
    @opts.freeze
    @mysql_configs = @opts[:mysql]
    @default_version = @opts[:default_version]
    host, user, password, port, socket =  %w{host user pass port socket}.map { |opt| @mysql_configs[@default_version][opt] }
    @pool = connection_pool_klass.new(:host => host, :username => user, :password => password, :database => "mysql", :port => port.to_i, :socket => socket, :logger => @logger, :pool => 20)
  end

  it "should provide mysql connections" do
    @pool.with_connection do |conn|
      expect {conn.query("select 1")}.to_not raise_error
    end
  end

  it "should not provide the same connection to different threads" do
    @pool.max = 20
    THREADS = 20
    ITERATES = 10
    threads = []
    Thread.abort_on_exception = true
    THREADS.times do
      thread  = Thread.new do
        ITERATES.times do
          begin
            @pool.with_connection do |conn|
              sleep_time = rand(5).to_f/10
              # if multiple threads acquire the same connection, following query would fail.
              conn.query("select sleep(#{sleep_time})")
            end
          end
        end
      end
      threads << thread
    end
    threads.each {|t| t.join}
    @pool.max = 5
  end

  it "should verify a connection before checkout" do
    host, user, password, port, socket =  %w{host user pass port socket}.map { |opt| @mysql_configs[@default_version][opt] }
    pool = connection_pool_klass.new(:host => host, :username => user, :password => password, :database => "mysql", :port => port.to_i, :socket => socket, :pool => 1, :logger => @logger)
    pool.max = 1

    pool.with_connection do |conn|
      conn.close
    end

    pool.with_connection do |conn|
      expect{conn.query("select 1")}.to_not raise_error
    end
  end

  it "should keep the pooled connection alive" do
    @pool.close
    # bypass checkout since it verifiy and reconnect the connection
    @pool.connections.each{|conn| conn.active?.should == nil }

    @pool.keep_alive
    @pool.connections.each{|conn| conn.active?.should == true}

    @pool.with_connection do |conn|
      conn.ping.should == true
    end
  end

  it "should report the mysql connection status" do
    mock_client = mock("client")
    mock_client.should_receive(:ping).and_return(true)
    mock_client.should_receive(:close).and_return(true)
    Mysql2::Client.should_receive(:new).and_return(mock_client)

    pool = connection_pool_klass.new(:logger => @logger, :pool => 1)
    pool.connected?.should == true

    error = Mysql2::Error.new("Can't connect to mysql")
    # Simulate mysql server is gone.
    mock_client.should_receive(:ping).and_return(nil)
    Mysql2::Client.should_receive(:new).and_raise(error)
    pool.connected?.should == nil
  end

  it "should not leak connection when can't connect to mysql" do
    mock_client = mock("client")
    mock_client.should_receive(:close).and_return(true)
    Mysql2::Client.should_receive(:new).and_return(mock_client)

    pool = connection_pool_klass.new(:logger => @logger, :pool => 1)

    # Simulate mysql server is gone.
    mock_client.should_receive(:ping).and_return(nil)
    error = Mysql2::Error.new("Can't connect to mysql")
    Mysql2::Client.should_receive(:new).and_raise(error)

    expect{ pool.with_connection{|conn| conn.query("select 1")} }.to raise_error(Mysql2::Error, /Can't connect to mysql/)

    # Ensure that we can still checkout from the pool
    mock_client.should_receive(:ping).and_return(true)
    mock_client.should_receive(:query).with("select 1").and_return(true)
    expect{ pool.with_connection{|conn| conn.query("select 1")} }.to_not raise_error
  end

  it "should raise error when pool is still empty after timeout second" do
    host, user, password, port, socket =  %w{host user pass port socket}.map { |opt| @mysql_configs[@default_version][opt] }
    # create a tiny pool with very short timeout
    pool = connection_pool_klass.new(:host => host, :username => user, :password => password, :database => "mysql",
                                     :port => port.to_i, :socket => socket, :pool => 1, :logger => @logger, :wait_timeout => 2)
    pool.max = 1
    threads = []
    threads << Thread.new do
      # acquire connection for quite a long time.
      pool.with_connection do |conn|
        sleep 5
        conn.query("select 1")
      end
    end

    error = nil
    threads << Thread.new do
      begin
        sleep 1
        pool.with_connection do |conn|
          conn.query("select 1")
        end
      rescue => e
        error = e
      end
    end
    threads.each{|t| t.join}
    error.should_not == nil
    error.to_s.should match(/could not obtain a database connection/)
  end

  it "should enlarge and shrink connection pool" do
    host, user, password, port, socket =  %w{host user pass port socket}.map { |opt| @mysql_configs[@default_version][opt] }
    # create a tiny pool with very short timeout
    pool = connection_pool_klass.new(:host => host, :username => user, :password => password, :database => "mysql",
                                     :port => port.to_i, :socket => socket, :pool => 1, :logger => @logger, :expire => 2,
                                     :pool_min => 1, :pool_max => 5)
    pool.connections.size.should == 1
    threads  = []
    6.times do
      threads << Thread.new do
        pool.with_connection do |conn|
          sleep(1)                    #make sure connections are created but not checked in
          conn.query("select 1")
        end
      end
    end
    threads.each(&:join)

    pool.connections.size.should == 5 #should be enlarged but not larger than max
    sleep(3)                          #wait for expiration of connections
    pool.keep_alive                   #remove the expired connections
    pool.connections.size.should == 1
  end

  it "should lazy shutdown mysql connection pool" do
    host, user, password, port, socket =  %w{host user pass port socket}.map { |opt| @mysql_configs[@default_version][opt] }
    pool = connection_pool_klass.new(:host => host, :username => user, :password => password, :database => "mysql",
                                     :port => port.to_i, :socket => socket, :pool => 1, :logger => @logger)
    Thread.new do
      pool.with_connection do |conn|
        sleep(0.5)
        conn.query("select 1")
      end
    end

    sleep(0.2) #wait for the thread to check out the connection
    pool.shutdown
    pool.shutting_down.should == true
    pool.connections.size.should > 0   #should still keep the connections
    pool.connections.each { |conn| conn.active?.should == true }

    expect do
      pool.with_connection do |conn|
        conn.query("select 1")
      end
    end.to raise_error(StandardError, /shutting down/)

    sleep(2)
    pool.connections.size.should == 0  #connections should have been closed and removed
  end
end
