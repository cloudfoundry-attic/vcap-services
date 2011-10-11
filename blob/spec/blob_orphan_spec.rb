# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require "spec_helper"

describe "blob_node check & purge orphan" do
  before :all do
    EM.run do
      @opts = get_node_config
      @logger = @opts[:logger]
      @node = Node.new(@opts)
      all_ins = @node.all_instances_list
      all_ins.each {|name| @node.unprovision(name,[])}
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
      (after_bindings - before_bindings).index{|credential| credential["username"] == ob["username"]}.should_not be_nil
      EM.stop
    end
  end

  it "should find out the orphans after check" do
    EM.run do
      oi = @node.provision("free")
      sleep 0.5
      ob = @node.bind(oi["name"],'rw')
      @node.check_orphan([])
      @node.orphan_ins_hash.values[0].include?(oi["name"]).should be_true
      @node.orphan_binding_hash.values[0].index{|credential| credential["username"] == ob["username"]}.should_not be_nil
      @node.unprovision(oi["name"],[])
      EM.stop
    end
  end

  it "should find out the orphaned binding that attaches to existing instance" do
    EM.run do
      oi = @node.provision("free")  # Assume this instance is not orphaned
      sleep 0.5
      ob = @node.bind(oi["name"],'rw')
      @node.check_orphan([{
        "service_id" => oi["name"],
        "configuration" => {},
        "credentials" => oi
      }])
      @node.orphan_binding_hash.values[0].index{|credential| credential["username"] == ob["username"]}.should_not be_nil
      @node.unprovision(oi["name"],[])
      EM.stop
    end
  end

  it "should be able to purge the orphan" do
    EM.run do
      oi = @node.provision("free")
      sleep 0.5
      ob = @node.bind(oi["name"],'rw')
      @node.purge_orphan([oi["name"]],[ob])
      @node.check_orphan([])
      @node.orphan_ins_hash.values[0].include?(oi["name"]).should be_false
      @node.orphan_binding_hash.values[0].index{|credential| credential["username"] == ob["username"]}.should be_nil
      EM.stop
    end
  end
end
