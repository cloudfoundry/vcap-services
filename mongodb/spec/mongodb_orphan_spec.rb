# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require "spec_helper"

describe "mongodb_node check & purge orphan" do
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

  it "should return proper instances list" do
    EM.run do
      before_list = @node.all_instances_list
      oi = @node.provision("free")
      after_list = @node.all_instances_list
      @node.unprovision(oi["name"],[])
      (after_list - before_list).include?(oi["name"]).should be_true
      EM.stop
    end
  end

  it "should find out the orphan instance after check" do
    EM.run do
      oi = @node.provision("free")
      @node.check_orphan([])
      @node.orphan_ins_hash.values[0].include?(oi["name"]).should be_true
      @node.unprovision(oi["name"],[])
      EM.stop
    end
  end

  it "should be able to purge the orphan" do
    EM.run do
      oi = @node.provision("free")
      @node.purge_orphan([oi["name"]],[])
      @node.check_orphan([])
      @node.orphan_ins_hash.values[0].include?(oi["name"]).should be_false
      EM.stop
    end
  end
end
