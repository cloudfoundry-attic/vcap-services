# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift File.dirname(__FILE__)
$:.unshift File.join(File.dirname(__FILE__), '..', '..')
$:.unshift File.join(File.dirname(__FILE__), '..', '..', 'lib')

require 'rubygems'
require 'rspec'
require 'logger'

require "base_spec_helper"
require "node_spec_helper"
require "provision_spec_helper"
require "async_gw_spec_helper"
require "backup_spec_helper"
require 'base/service_message'
require 'base/service_error'

TEST_NODE_ID = "node-1"
TEST_PURGE_INS_HASH =
{
    "#{TEST_NODE_ID}" => [
      "n1_orphan_1",
      "n1_orphan_2"
    ],
    "#{TEST_NODE_ID}2" => [
      "n2_orphan_1",
      "n2_orphan_2"
    ]
}
TEST_PURGE_BIND_HASH =
{
  "#{TEST_NODE_ID}"  => [
    {#binding to orphan instance
      "name"     => "n1_orphan_1",
      "hostname" => "127.0.0.1",
      "host"     => "127.0.0.1",
      "port"     => 3306,
      "user"     => "n1_orphan_user_1",
      "username" => "n1_orphan_user_1",
      "password" => "*"
    },
    {#binding to orphan instance
      "name"     => "n1_orphan_1",
      "hostname" => "127.0.0.1",
      "host"     => "127.0.0.1",
      "port"     => 3306,
      "user"     => "n1_orphan_user_2",
      "username" => "n1_orphan_user_2",
      "password" => "*"
    }
  ],
  "#{TEST_NODE_ID}2" => [
    {#binding to orphan instance
      "name"     => "n2_orphan_1",
      "hostname" => "127.0.0.2",
      "host"     => "127.0.0.2",
      "port"     => 3306,
      "user"     => "n2_orphan_user_1",
      "username" => "n2_orphan_user_1",
      "password" => "*"
    },
    {#orphan binding
      "name"     => "n2_orphan_3",
      "hostname" => "127.0.0.2",
      "host"     => "127.0.0.2",
      "port"     => 3306,
      "user"     => "n2_orphan_user_3",
      "username" => "n2_orphan_user_3",
      "password" => "*"
    }
  ]
}

TEST_CHECK_HANDLES =
[
  {
    "service_id"    => "20".ljust(36, "I"),
    "configuration" => {
      "plan" => "free"
    },
    "credentials"   => {
      "name"     => "20".ljust(36, "I"),
      "hostname" => "127.0.0.1",
      "host"     => "127.0.0.1",
      "port"     => 3306,
      "user"     => "umLImcKDtRtID",
      "username" => "umLImcKDtRtID",
      "password" => "p1ZivmGDSJXSC",
      "node_id"  => "node-2"
    }
  },
  {
    "service_id"    => "30".ljust(36, "I"),
    "configuration" => {
      "plan" => "free"
    },
    "credentials"   => {
      "name"     => "30".ljust(36, "I"),
      "hostname" => "127.0.0.1",
      "host"     => "127.0.0.1",
      "port"     => 3306,
      "user"     => "umLImcKDtRtIu",
      "username" => "umLImcKDtRtIu",
      "password" => "p1ZivmGDSJXSC",
      "node_id"  => "node-3"
    }
  },
  {
    "service_id"    => "id_for_other_node_ins",
    "configuration" => {
      "plan" => "free"
    },
    "credentials"   => {
      "name"     => "id_for_other_node_ins",
      "hostname" => "127.0.0.2",
      "host"     => "127.0.0.2",
      "port"     => 3306,
      "user"     => "ffffxWPUcNxS4",
      "username" => "ffffxWPUcNxS4",
      "password" => "ffffg73QpVDSV",
      "node_id"  => "node-x"
    }
  },
  {
    "service_id"    => "feli4831-3f53-4119-8f3f-8d34645aaf5d",
    "configuration" => {
      "plan" => "free",
      "data" => {
        "binding_options" => {}
      }
    },
    "credentials"   => {
      "name"     => "20".ljust(36, "I"),
      "hostname" => "127.0.0.1",
      "host"     => "127.0.0.1",
      "port"     => 1,
      "db"       => "db2",
      "user"     => "20".ljust(18, "U"),
      "username" => "20".ljust(18, "U"),
      "password" => "ff9bMF25hwtlS"
    }
  },
  {
    "service_id"    => "aad74831-3f53-4119-8f3f-8d34645aaf5d",
    "configuration" => {
      "plan" => "free",
      "data" => {
        "binding_options" => {}
      }
    },
    "credentials"   => {
      "name"     => "30".ljust(36, "I"),
      "hostname" => "127.0.0.1",
      "host"     => "127.0.0.1",
      "port"     => 1,
      "db"       => "db3",
      "user"     => "30".ljust(18, "U"),
      "username" => "30".ljust(18, "U"),
      "password" => "pl9bMF25hwtlS"
    }
  },
  {
    "service_id"    => "ffffffff-ce97-4cf8-afa8-a85a63d379b5",
    "configuration" => {
      "plan" => "free",
      "data" => {
        "binding_options" => {}
      }
    },
    "credentials"   => {
      "name"     => "id_for_other_node_ins",
      "hostname" => "127.0.0.2",
      "host"     => "127.0.0.2",
      "port"     => 3306,
      "user"     => "username_for_other_node_binding",
      "username" => "username_for_other_node_binding",
      "password" => "ffffntBqZojMo"
    }
  }
]

module Do
  # the tests below do various things then wait for something to
  # happen -- so there's a potential for a race condition.  to
  # minimize the risk of the race condition, increase this value (0.1
  # seems to work about 90% of the time); but to make the tests run
  # faster, decrease it
  STEP_DELAY = 0.5

  def self.at(index, &blk)
    EM.add_timer(index*STEP_DELAY) { blk.call if blk }
  end

  # Respect the real seconds while doing concurrent testing
  def self.sec(index, &blk)
    EM.add_timer(index) { blk.call if blk }
  end
end

def generate_ins_list(count)
  list = []
  count.times do |i|
    list << i.to_s.ljust(36, "I")
  end
  list
end

def generate_bind_list(count)
  list = []
  count.times do |i|
    list << {
      "name" => i.to_s.ljust(36, "I"),
      "username" => i.to_s.ljust(18, "U"),
      "port" => i,
      "db" => i.to_s.ljust(9, "D")
    }
  end
  list
end
