# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require "spec_helper"

describe "provision multiple versions" do
  MAX_CONNECTIONS = 100

  before :all do
    EM.run do
      @opts = get_node_config()
      @logger = @opts[:logger]
      @supported_versions = @opts[:supported_versions]

      @node = Node.new(@opts)
      @node.max_clients = MAX_CONNECTIONS

      EM.add_timer(1) { EM.stop }
    end
  end

  it "should allow provisioning all supported versions" do
    EM.run do
      @supported_versions.each do |v|
        resp = @node.provision("free", nil, v)

        conn = Mongo::Connection.new('localhost', resp['port'])
        version = conn.server_version.to_s
        conn.close

        version.start_with?(v).should be == true

        @node.unprovision(resp['name'], [])
      end
      EM.stop
    end
  end

  it "should throw unsupported version exception if provision for an unsupported version is requested" do
    EM.run do
      thrown = nil
      begin
        @node.provision("free", nil, "non_existent")
      rescue => e
        thrown = e.to_s
      end
      thrown.should_not be_nil
      thrown.match(/30004/) == true # ServiceError::UNSUPPORTED_VERSION
      EM.stop
    end
  end

end

