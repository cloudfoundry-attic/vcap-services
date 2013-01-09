# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require 'spec_helper'
require 'rack/test'
require 'eventmachine'

describe 'Serialization data server -- web server' do
  include Rack::Test::Methods

  before :all do
    @opts = load_config
    @opts[:serialization_base_dir] = "/tmp/spec_test_sds"
    @opts[:host] = "localhost"
    @opts[:port] = 20000
    @opts[:nginx] = {
      "nginx_path" => "/tmp",
      "nginx_port" => "8080"
    }
    @service = "mysql"
    @service_id = "abcd19841019"
    @snapshot_id = "22"
    @snapshot_path = File.join(@opts[:serialization_base_dir], "snapshots",
                               @service, @service_id[0,2], @service_id[2,2],
                               @service_id[4,2], @service_id, @snapshot_id)
    FileUtils.mkdir_p("/tmp/sdsupload")
    FileUtils.mkdir_p(@snapshot_path)
    @token = nil
    @server = nil
  end

  it "store uploaded file" do
    EM.run do
      ori_file_path = Tempfile.new('sds_file', '/tmp/sdsupload').path
      EM.add_timer(0) { @server = SDSTests.createSDS(@opts); @server.start() }
      EM.add_timer(1) { @server.send_store(@service, @service_id, ori_file_path) }
      EM.add_timer(6) { @server.stop; EM.stop }
    end
    @server.response.should == 200
  end

  it "get uploaded file" do
    EM.run do
      ori_file_path = Tempfile.new('sds_file', '/tmp/sdsupload').path
      EM.add_timer(0) { @server = SDSTests.createSDS(@opts); @server.start() }
      EM.add_timer(1) { @server.send_store(@service, @service_id, ori_file_path) }
      EM.add_timer(5) do
        @server.response.should == 200
        url = JSON.parse(@server.response_url)["url"]
        @token = url.gsub(/http:.*token=/, '')
      end
      EM.add_timer(7) { @server.get_file(@service, @service_id,  @token) }
      EM.add_timer(10) { @server.stop; EM.stop }
    end
    @server.response.should == 200
  end

  it "delete uploaded file" do
    EM.run do
      ori_file_path = Tempfile.new('sds_file', '/tmp/sdsupload').path
      EM.add_timer(0) { @server = SDSTests.createSDS(@opts); @server.start() }
      EM.add_timer(1) { @server.send_store(@service, @service_id, ori_file_path) }
      EM.add_timer(5) do
        @server.response.should == 200
        url = JSON.parse(@server.response_url)["url"]
        @token = url.gsub(/http:.*token=/, '')
      end
      EM.add_timer(7) { @server.delete_file(@service, @service_id,  @token) }
      EM.add_timer(10) { @server.stop; EM.stop }
    end
    @server.response.should == 200
  end

  it "get snapshot file" do
    EM.run do
      ori_file_path = Tempfile.new('sds_file', '/tmp/sdsupload').path
      EM.add_timer(0) { @server = SDSTests.createSDS(@opts); @server.start() }
      EM.add_timer(1) { @server.send_store(@service, @service_id, ori_file_path) }
      EM.add_timer(5) do
        @server.response.should == 200
        url = JSON.parse(@server.response_url)["url"]
        @token = url.gsub(/http:.*token=/, '')
        FileUtils.touch "#{@snapshot_path}/#{@token}"
      end
      EM.add_timer(7) { @server.get_snapshot(@service, @service_id, @snapshot_id, @token) }
      EM.add_timer(10) { @server.stop; EM.stop }
    end
    @server.response.should == 200
  end

  after :all do
    FileUtils.rm_rf("/tmp/sdsupload/*")
    FileUtils.rm_rf("/tmp/spec_test_sds")
  end
end
