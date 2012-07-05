# Copyright (c) 2009-2011 VMware, Inc.
require "spec_helper"

describe "elasticsearch_node check & purge orphan" do

  before :all do
    EM.run do
      @opts = get_node_config
      @logger = @opts[:logger]
      @node = Node.new(@opts)
      EM.stop
    end
  end

  it "should return proper instances & bindings list" do
    before_instances = nil, after_instances = nil
    before_bindings = nil, after_bindings = nil
    EM.run do
      before_instances = @node.all_instances_list
      before_bindings = @node.all_bindings_list
      @resp = @node.provision("free")
      EM.add_timer(5) do
        @bind_resp = @node.bind(@resp["name"], 'rw')
        EM.add_timer(1) do
          after_instances = @node.all_instances_list
          after_bindings = @node.all_bindings_list
        end
        EM.add_timer(2) do
          @node.unprovision(@resp["name"])
          EM.stop
        end
      end
    end
    (after_instances - before_instances).include?(@resp["name"]).should be_true
    (after_bindings - before_bindings).index { |credential| credential["username"] == @bind_resp["username"] }.should_not be_nil
  end

  it "should be able to purge the orphan" do
    EM.run do
      @resp = @node.provision("free")
      EM.add_timer(5) do
        @bind_resp = @node.bind(@resp["name"],'rw')
        EM.add_timer(1) do
          @node.purge_orphan([@resp["name"]], [@bind_resp])
        end
        EM.add_timer(2) do
          @node.all_instances_list.include?(@resp["name"]).should be_false
          @node.all_bindings_list.index { |credential| credential["username"] == @bind_resp["username"] }.should be_nil
          EM.stop
        end
      end
    end
  end

  after:all do
    EM.run do
      begin
        @node.shutdown()
        EM.stop
      rescue
      end
    end
    FileUtils.rm_rf(File.dirname(@opts[:base_dir]))
  end

end
