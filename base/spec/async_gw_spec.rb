# Copyright (c) 2009-2011 VMware, Inc.
require 'helper/spec_helper'
require 'eventmachine'

describe AsyncGatewayTests do

  it "should invoke check_orphan in check_orphan_interval time" do
    cc = nil
    gateway = nil
    EM.run do
      Do.at(0) { cc = AsyncGatewayTests.create_cloudcontroller ; cc.start }
      Do.at(1) { gateway = AsyncGatewayTests.create_check_orphan_gateway(true, 5, 3); gateway.start }
      Do.at(20) { cc.stop ; gateway.stop ; EM.stop }
    end
    gateway.check_orphan_invoked.should be_true
    gateway.double_check_orphan_invoked.should be_true
  end

  it "should be able to purge_orphan" do
    cc = nil
    gateway = nil
    EM.run do
      Do.at(0) { cc = AsyncGatewayTests.create_cloudcontroller ; cc.start }
      Do.at(1) { gateway = AsyncGatewayTests.create_nice_gateway ; gateway.start }
      Do.at(2) { gateway.send_purge_orphan_request}
      Do.at(3) { cc.stop ; gateway.stop ; EM.stop }
    end
    gateway.purge_orphan_http_code.should == 200
  end

  it "should be able to return error when purge_orphan failed" do
    cc = nil
    gateway = nil
    EM.run do
      Do.at(0) { cc = AsyncGatewayTests.create_cloudcontroller ; cc.start }
      Do.at(1) { gateway = AsyncGatewayTests.create_nasty_gateway ; gateway.start }
      Do.at(2) { gateway.send_purge_orphan_request}
      Do.at(3) { cc.stop ; gateway.stop ; EM.stop }
    end
    gateway.purge_orphan_http_code.should == 500
  end

  it "should be able to check_orphan" do
    cc = nil
    gateway = nil
    EM.run do
      Do.at(0) { cc = AsyncGatewayTests.create_cloudcontroller ; cc.start }
      Do.at(1) { gateway = AsyncGatewayTests.create_check_orphan_gateway(true, 10, 3); gateway.start }
      Do.at(2) { gateway.send_check_orphan_request}
      Do.at(10) { cc.stop ; gateway.stop ; EM.stop }
    end
    gateway.check_orphan_invoked.should be_true
    gateway.double_check_orphan_invoked.should be_true
    gateway.check_orphan_http_code.should == 200
  end

  it "should be able to return error when check_orphan failed" do
    cc = nil
    gateway = nil
    EM.run do
      Do.at(0) { cc = AsyncGatewayTests.create_cloudcontroller ; cc.start }
      Do.at(1) { gateway = AsyncGatewayTests.create_nasty_gateway ; gateway.start }
      Do.at(2) { gateway.send_check_orphan_request}
      Do.at(10) { cc.stop ; gateway.stop ; EM.stop }
    end
    gateway.check_orphan_invoked.should be_true
    gateway.double_check_orphan_invoked.should be_false
    gateway.check_orphan_http_code.should == 500
  end

  it "should be able to provision" do
    cc = nil
    gateway = nil
    EM.run do
      Do.at(0) { cc = AsyncGatewayTests.create_cloudcontroller ; cc.start }
      Do.at(1) { gateway = AsyncGatewayTests.create_nice_gateway ; gateway.start }
      Do.at(2) { gateway.send_provision_request }
      Do.at(3) { cc.stop ; gateway.stop ; EM.stop }
    end
    gateway.provision_http_code.should == 200
  end

  it "should be able to unprovision" do
    cc = nil
    gateway = nil
    EM.run do
      Do.at(0) { cc = AsyncGatewayTests.create_cloudcontroller ; cc.start }
      Do.at(1) { gateway = AsyncGatewayTests.create_nice_gateway ; gateway.start }
      Do.at(2) { gateway.send_provision_request }
      Do.at(3) { gateway.send_unprovision_request }
      Do.at(4) { cc.stop ; gateway.stop ; EM.stop }
    end
    gateway.provision_http_code.should == 200
    gateway.unprovision_http_code.should == 200
  end

  it "should be able to bind" do
    cc = nil
    gateway = nil
    EM.run do
      Do.at(0) { cc = AsyncGatewayTests.create_cloudcontroller ; cc.start }
      Do.at(1) { gateway = AsyncGatewayTests.create_nice_gateway ; gateway.start }
      Do.at(2) { gateway.send_provision_request }
      Do.at(3) { gateway.send_bind_request }
      Do.at(4) { cc.stop ; gateway.stop ; EM.stop }
    end
    gateway.provision_http_code.should == 200
    gateway.bind_http_code.should == 200
  end

  it "should be able to unbind" do
    cc = nil
    gateway = nil
    EM.run do
      Do.at(0) { cc = AsyncGatewayTests.create_cloudcontroller ; cc.start }
      Do.at(1) { gateway = AsyncGatewayTests.create_nice_gateway ; gateway.start }
      Do.at(2) { gateway.send_provision_request }
      Do.at(3) { gateway.send_bind_request }
      Do.at(4) { gateway.send_unbind_request }
      Do.at(5) { cc.stop ; gateway.stop ; EM.stop }
    end
    gateway.provision_http_code.should == 200
    gateway.bind_http_code.should == 200
    gateway.unbind_http_code.should == 200
  end

  it "should be able to restore" do
    cc = nil
    gateway = nil
    EM.run do
      Do.at(0) { cc = AsyncGatewayTests.create_cloudcontroller ; cc.start }
      Do.at(1) { gateway = AsyncGatewayTests.create_nice_gateway ; gateway.start }
      Do.at(2) { gateway.send_restore_request('s_id') }
      Do.at(3) { cc.stop ; gateway.stop ; EM.stop }
    end
    gateway.restore_http_code.should == 200
  end

  it "should be able to recover" do
    cc = nil
    gateway = nil
    EM.run do
      Do.at(0) { cc = AsyncGatewayTests.create_cloudcontroller ; cc.start }
      Do.at(1) { gateway = AsyncGatewayTests.create_nice_gateway ; gateway.start }
      Do.at(2) { gateway.send_recover_request }
      Do.at(3) { cc.stop ; gateway.stop ; EM.stop }
    end
    gateway.recover_http_code.should == 200
  end

  it "should be able to migrate instance" do
    cc = nil
    gateway = nil
    EM.run do
      Do.at(0) { cc = AsyncGatewayTests.create_cloudcontroller ; cc.start }
      Do.at(1) { gateway = AsyncGatewayTests.create_nice_gateway ; gateway.start }
      Do.at(2) { gateway.send_migrate_request }
      Do.at(3) { cc.stop ; gateway.stop ; EM.stop }
    end
    gateway.migrate_http_code.should == 200
  end

  it "should be able to get instance id list" do
    cc = nil
    gateway = nil
    EM.run do
      Do.at(0) { cc = AsyncGatewayTests.create_cloudcontroller ; cc.start }
      Do.at(1) { gateway = AsyncGatewayTests.create_nice_gateway ; gateway.start }
      Do.at(2) { gateway.send_instances_request }
      Do.at(3) { cc.stop ; gateway.stop ; EM.stop }
    end
    gateway.instances_http_code.should == 200
  end

  it "should not serve request when handle is not fetched" do
    gateway = nil
    EM.run do
      # We don't start cc here, so gateway will fail to fetch handles
      Do.at(0) { gateway = AsyncGatewayTests.create_nice_gateway ; gateway.start }
      Do.at(2) { gateway.send_provision_request }
      Do.at(3) { gateway.stop ; EM.stop }
    end
    gateway.provision_http_code.should == 503
  end

  it "should work if provisioner finishes within timeout" do
    cc = nil
    gateway = nil
    EM.run do
      Do.at(0) { cc = AsyncGatewayTests.create_cloudcontroller ; cc.start }
      Do.at(1) { gateway = AsyncGatewayTests.create_timeout_gateway(true, 3) ; gateway.start }
      Do.at(2) { gateway.send_provision_request }
      Do.at(13) { cc.stop ; gateway.stop ; EM.stop }
    end
    gateway.provision_http_code.should == 200
  end

  it "should be able to report timeout if provisioner cannot finish within timeout" do
    cc = nil
    gateway = nil
    EM.run do
      Do.at(0) { cc = AsyncGatewayTests.create_cloudcontroller ; cc.start }
      Do.at(1) { gateway = AsyncGatewayTests.create_timeout_gateway(false, 3) ; gateway.start }
      Do.at(2) { gateway.send_provision_request }
      Do.at(13) { cc.stop ; gateway.stop ; EM.stop }
    end
    gateway.provision_http_code.should == 503
  end

  it "should be able to return error when provision failed" do
    cc = nil
    gateway = nil
    EM.run do
      Do.at(0) { cc = AsyncGatewayTests.create_cloudcontroller ; cc.start }
      Do.at(1) { gateway = AsyncGatewayTests.create_nasty_gateway ; gateway.start }
      Do.at(2) { gateway.send_provision_request }
      Do.at(3) { cc.stop ; gateway.stop ; EM.stop }
    end
    gateway.provision_http_code.should == 500
  end

  it "should be able to return error when unprovision failed" do
    cc = nil
    gateway = nil
    EM.run do
      Do.at(0) { cc = AsyncGatewayTests.create_cloudcontroller ; cc.start }
      Do.at(1) { gateway = AsyncGatewayTests.create_nasty_gateway ; gateway.start }
      Do.at(2) { gateway.send_unprovision_request('s_id') }
      Do.at(3) { cc.stop ; gateway.stop ; EM.stop }
    end
    gateway.unprovision_http_code.should == 500
  end

  it "should be able to return error when bind failed" do
    cc = nil
    gateway = nil
    EM.run do
      Do.at(0) { cc = AsyncGatewayTests.create_cloudcontroller ; cc.start }
      Do.at(1) { gateway = AsyncGatewayTests.create_nasty_gateway ; gateway.start }
      Do.at(2) { gateway.send_bind_request('s_id') }
      Do.at(3) { cc.stop ; gateway.stop ; EM.stop }
    end
    gateway.bind_http_code.should == 500
  end

  it "should be able to return error when unbind failed" do
    cc = nil
    gateway = nil
    EM.run do
      Do.at(0) { cc = AsyncGatewayTests.create_cloudcontroller ; cc.start }
      Do.at(1) { gateway = AsyncGatewayTests.create_nasty_gateway ; gateway.start }
      Do.at(2) { gateway.send_unbind_request('s_id', 'b_id') }
      Do.at(3) { cc.stop ; gateway.stop ; EM.stop }
    end
    gateway.unbind_http_code.should == 500
  end

  it "should be able to return error when restore failed" do
    cc = nil
    gateway = nil
    EM.run do
      Do.at(0) { cc = AsyncGatewayTests.create_cloudcontroller ; cc.start }
      Do.at(1) { gateway = AsyncGatewayTests.create_nasty_gateway ; gateway.start }
      Do.at(2) { gateway.send_restore_request('s_id') }
      Do.at(3) { cc.stop ; gateway.stop ; EM.stop }
    end
    gateway.restore_http_code.should == 500
  end

  it "should be able to return error when recover failed" do
    cc = nil
    gateway = nil
    EM.run do
      Do.at(0) { cc = AsyncGatewayTests.create_cloudcontroller ; cc.start }
      Do.at(1) { gateway = AsyncGatewayTests.create_nasty_gateway ; gateway.start }
      Do.at(2) { gateway.send_recover_request }
      Do.at(3) { cc.stop ; gateway.stop ; EM.stop }
    end
    gateway.recover_http_code.should == 500
  end

  it "should be able to return error when migrate instance failed" do
    cc = nil
    gateway = nil
    EM.run do
      Do.at(0) { cc = AsyncGatewayTests.create_cloudcontroller ; cc.start }
      Do.at(1) { gateway = AsyncGatewayTests.create_nasty_gateway ; gateway.start }
      Do.at(2) { gateway.send_migrate_request }
      Do.at(3) { cc.stop ; gateway.stop ; EM.stop }
    end
    gateway.migrate_http_code.should == 500
  end

  it "should be able to return error when get instance id list failed" do
    cc = nil
    gateway = nil
    EM.run do
      Do.at(0) { cc = AsyncGatewayTests.create_cloudcontroller ; cc.start }
      Do.at(1) { gateway = AsyncGatewayTests.create_nasty_gateway ; gateway.start }
      Do.at(2) { gateway.send_instances_request }
      Do.at(3) { cc.stop ; gateway.stop ; EM.stop }
    end
    gateway.instances_http_code.should == 500
  end
end
