# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require "spec_helper"

describe "mongodb_node check & purge orphan" do
  before :all do
    @opts = get_node_config
    @logger = @opts[:logger]
    EM.run do
      @node = Node.new(@opts)
      EM.add_timer(1) { EM.stop }
    end
  end

  after :all do
    @node.shutdown
    FileUtils.rm_rf(File.dirname(@opts[:base_dir]))
  end

  it "should return proper instances & bindings list" do
    before_instances = @node.all_instances_list
    before_bindings = @node.all_bindings_list
    oi = @node.provision("free")
    ob = @node.bind(oi["name"],'rw')
    after_instances = @node.all_instances_list
    after_bindings = @node.all_bindings_list
    @node.unprovision(oi["name"], [])
    (after_instances - before_instances).include?(oi["name"]).should be_true
    (after_bindings - before_bindings).index { |credential| credential["username"] == ob["username"] }.should_not be_nil
  end

  it "should be able to purge the orphan" do
    oi = @node.provision("free")
    ob = @node.bind(oi["name"],'rw')
    @node.purge_orphan([oi["name"]], [ob])
    @node.all_instances_list.include?(oi["name"]).should be_false
    @node.all_bindings_list.index { |credential| credential["username"] == ob["username"] }.should be_nil
  end
end
