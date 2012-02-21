# Copyright (c) 2009-2011 VMware, Inc.
require 'spec_helper'

require 'eventmachine'

describe BackupTest do
  it "should exit after receive TERM signal" do
    EM.run do
      pid = Process.pid
      backup = BackupTest::ZombieBackup.new
      EM.defer do
        backup.start
      end
      EM.add_timer(1) { Process.kill("TERM", pid) }
      EM.add_timer(5) do
        backup.exit_invoked.should be_true
        EM.stop
      end
    end
  end
end
