# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require 'spec_helper'

describe BackupManagerTests do
  it "should be able to terminated while manager is sleeping" do
    EM.run do
      pid = Process.pid
      manager = BackupManagerTests.create_manager("empty", "backups")
      EM.defer do
        manager.start rescue nil
      end
      EM.add_timer(1) { Process.kill("TERM", pid) }
      EM.add_timer(5) do
        manager.shutdown_invoked.should be_true
        EM.stop
      end
    end
  end

  it "should be able to terminated while manager is running" do
    EM.run do
      pid = Process.pid
      manager = BackupManagerTests.create_manager("cc_test", "backups")
      EM.defer do
        manager.start rescue nil
      end
      EM.add_timer(4) { Process.kill("TERM", pid) }
      EM.add_timer(10) do
        manager.shutdown_invoked.should be_true
        EM.stop
      end
    end
  end

  it "should be able to generate varz_details" do
    EM.run do
      manager = BackupManagerTests.create_manager("empty", "backups")
      EM.add_timer(1) do
        varz = manager.varz_details
        varz[:disk_total_size].length.should > 0
        varz[:disk_used_size].length.should > 0
        varz[:disk_available_size].length.should > 0
        varz[:disk_percentage].length.should > 0
        EM.stop
      end
    end
  end
end

