# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require "spec_helper"

describe "vblob_node check & purge orphan" do
  before :all do
    EM.run do
      @opts = get_node_config
      @logger = @opts[:logger]
      @node = Node.new(@opts)
      EM.stop
    end
  end

  it "should return proper instances & bindings list" do
    EM.run do
      before_instances = @node.all_instances_list
      before_bindings = @node.all_bindings_list
      oi = @node.provision("free")
      sleep 0.5
      ob = @node.bind(oi["name"],'rw')
      after_instances = @node.all_instances_list
      after_bindings = @node.all_bindings_list
      @node.unprovision(oi["name"],[])
      (after_instances - before_instances).include?(oi["name"]).should be_true
      (after_bindings - before_bindings).index { |credential| credential["username"] == ob["username"] }.should_not be_nil
      EM.stop
    end
  end

  it "should be able to purge the orphan" do
    EM.run do
      oi = @node.provision("free")
      sleep 0.5
      ob = @node.bind(oi["name"],'rw')
      @node.purge_orphan([oi["name"]],[ob])
      @node.all_instances_list.include?(oi["name"]).should be_false
      @node.all_bindings_list.index { |credential| credential["username"] == ob["username"] }.should be_nil
      EM.stop
    end
  end
end
