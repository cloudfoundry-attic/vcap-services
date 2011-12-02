# Copyright (c) 2009-2011 VMware, Inc.
require "aws/s3"
$:.unshift(File.dirname(__FILE__))
require "spec_helper"

describe "blob_node provision" do

  before :all do
    EM.run do
      @opts = get_node_config()
      @logger = @opts[:logger]
      @node = Node.new(@opts)
      @original_memory = @node.available_memory

      EM.add_timer(2) { @resp = @node.provision("free") }
      EM.add_timer(4) { 
        AWS::S3::Base.establish_connection!(
          :access_key_id     => @resp['username'],
          :secret_access_key => @resp['password'],
          :server            => @resp['host'],
          :port              => @resp['port']
        )
      }
      EM.add_timer(6) { EM.stop }
    end
  end

  it "should have valid response" do
    @resp.should_not be_nil
    puts @resp
    inst_name = @resp['name']
    inst_name.should_not be_nil
    inst_name.should_not == ""
  end

  it "should consume node's memory" do
    (@original_memory - @node.available_memory).should > 0
  end

  it "should be able to connect to blob gateway" do
    is_port_open?('127.0.0.1',@resp['port']).should be_true
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
      stats[:running_services][0]['name'].should_not be_nil
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

  it "should reject unauthorized access" do
    response = nil
    err = nil
    EM.run do
        EM.add_timer(0) { begin 
                            response = `curl http://127.0.0.1:#{@resp['port']}/ -s`
                          rescue => e
                            err = e
                          end
                        }
        EM.add_timer(1) { EM.stop }
    end
    err.should be_nil
    response.should_not be_nil
    response.should == "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>Unauthorized</Code><Message>Signature does not match</Message></Error>"
  end

  it "should be able to create bucket" do
    rc = nil
    EM.run do
      begin
        EM.add_timer(0) { rc = AWS::S3::Bucket.create 'blob-unit-test' }
        EM.add_timer(1) { EM.stop }
      rescue Exception => e
        @logger.debug e
      end
      e.should be_nil
    end
    rc.should == true
  end

  it "should be able to alert when creating object against a non-existent bucket" do
    err = nil
    EM.run do
        EM.add_timer(0) { 
          begin
            AWS::S3::S3Object.create('file1', 'hello world!', 'blob-unit-test2') 
          rescue => e
            err = e
          end
        } 
        EM.add_timer(1) { EM.stop }
    end
    err.should_not be_nil
  end

  it "should be able to create object against an existent bucket" do
    response = nil
    EM.run do
      begin
        EM.add_timer(0) { AWS::S3::S3Object.create('file1','hello world!', 'blob-unit-test') } 
        EM.add_timer(1) { response = AWS::S3::S3Object.value 'file1','blob-unit-test' }
        EM.add_timer(2) { EM.stop }
      rescue Exception => e
        @logger.debug e
      end
      e.should be_nil
    end
      response.should_not be_nil
      response.should == 'hello world!'
  end

  it "should be able to alert when deleting a non-existent object" do
    err = nil
    response = nil
    EM.run do
        EM.add_timer(0) { 
          begin
            response = AWS::S3::S3Object.delete 'file2', 'blob-unit-test'
          rescue => e
            err = e
          end 
        }
        EM.add_timer(1) { EM.stop }
    end
    err.should be_nil
    response.should == true
  end

  it "should be able to delete an existent object" do
    response = nil
    EM.run do
      begin
        AWS::S3::S3Object.delete 'file1','blob-unit-test'
        response = AWS::S3::S3Object.value 'file1','blob-unit-test'
        EM.stop
      rescue Exception => e
        @logger.debug e
        EM.stop
      end
      e.should_not be_nil
    end
    response.should be_nil
  end

  it "should keep the result after node restart" do
    port_open_1 = nil
    port_open_2 = nil
    EM.run do
      EM.add_timer(0) { @node.shutdown }
      EM.add_timer(1) { port_open_1 = is_port_open?('127.0.0.1', @resp['port'])
                      }
      EM.add_timer(2) { @node = Node.new(@opts) }
      EM.add_timer(3) { port_open_2 = is_port_open?('127.0.0.1', @resp['port'])
                      }
      EM.add_timer(4) { EM.stop }
    end

    port_open_1.should be_false
    port_open_2.should be_true
    response = AWS::S3::Service.buckets
    response.should_not be_nil
    response.size.should == 1
  end

  it "should be able to delete an empty bucket" do
    EM.run do
      begin
        AWS::S3::Bucket.delete 'blob-unit-test' 
      rescue Exception => e
        @logger.debug e
      end
      e.should be_nil
      EM.stop
    end
    EM.run do
      begin
        AWS::S3::Bucket.find 'blob-unit-test'
      rescue => e
      end
      e.should_not be_nil
      EM.stop
    end
  end

  it "should return error when unprovisioning a non-existent instance" do
    EM.run do
      e = nil
      begin
        @node.unprovision('not existent', [])
      rescue => e
      end
      e.should_not be_nil
      EM.stop
    end
  end

  # unprovision here
  it "should be able to unprovision an existent instance" do
    EM.run do
      @node.unprovision(@resp['name'], [])

      e = nil
      begin
        AWS::S3::Service.buckets
      rescue => e
      end
      e.should be_nil
      EM.stop
    end
  end

  it "should release memory" do
    EM.run do
      @original_memory.should == @node.available_memory
      EM.stop
    end
  end

end


