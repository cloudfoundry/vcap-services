# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require "spec_helper"

describe "blob_node provision" do

  before :all do
    EM.run do
      @opts = get_node_config()
      @logger = @opts[:logger]
      @node = Node.new(@opts)
      @original_memory = @node.available_memory

      EM.add_timer(2) { @resp = @node.provision("free") }
      EM.add_timer(4) { EM.stop }
    end
  end

  it "should have valid response" do
    @resp.should_not be_nil
    puts @resp
    inst_name = @resp['name']
    inst_name.should_not be_nil
    inst_name.should_not == ""
  end

  it "should consume node's memory" do
    (@original_memory - @node.available_memory).should > 0
  end

  it "should be able to connect to mongodb" do
    is_port_open?('127.0.0.1', @resp['meta_port']).should be_true
  end

  it "should be able to connect to blob gateway" do
    is_port_open?('127.0.0.1',@resp['port']).should be_true
  end

  it "should not allow unauthorized user to access the instance meta" do
    EM.run do
      begin
        conn = Mongo::Connection.new('localhost', @resp['meta_port'])
        db = conn.db(@resp['db'])
        coll = db.collection('mongo_unit_test')
        coll.insert({'a' => 1})
        coll.count()
      rescue Exception => e
        @logger.debug e
      ensure
        conn.close if conn
      end
      e.should_not be_nil
      EM.stop
    end
  end

  it "should return varz" do
    EM.run do
      stats = nil
      10.times do
        stats = @node.varz_details
        @node.healthz_details
      end
      stats.should_not be_nil
      stats[:running_services].length.should > 0
      stats[:running_services][0]['name'].should_not be_nil
      stats[:running_services][0]['db'].should_not be_nil
      stats[:running_services][0]['overall']['connections']['current'].should == 2
      stats[:disk].should_not be_nil
      stats[:services_max_memory].should > 0
      stats[:services_used_memory].should > 0
      EM.stop
    end
  end

  it "should return healthz" do
    EM.run do
      stats = @node.healthz_details
      stats.should_not be_nil
      stats[:self].should == "ok"
      stats[@resp['name'].to_sym].should == "ok"
      EM.stop
    end
  end

  it "should be able to create bucket" do
    response = nil
    EM.run do
      begin
        EM.add_timer(1) { `curl http://127.0.0.1:#{@resp['port']}/blob_unit_test --request PUT -s`}
        EM.add_timer(6) { response = `curl http://127.0.0.1:#{@resp['port']}/blob_unit_test/ -s`}
        EM.add_timer(7) { EM.stop }
      rescue Exception => e
        @logger.debug e
      end
      e.should be_nil
    end
      response.should_not be_nil
      response.should == '{"ListBucketResult":{"Name":"blob_unit_test","Prefix":{},"Marker":{},"MaxKeys":"1000","IsTruncated":"false"}}'
  end

  it "should be able to alert when creating object against a non-existing bucket" do
    response = nil
    EM.run do
      begin
        EM.add_timer(0) { response = `curl http://127.0.0.1:#{@resp['port']}/blob_unit_test2/testfile1 --request PUT -T testfile1.txt -s`}
        EM.add_timer(1) { EM.stop }
      rescue Exception => e
        @logger.debug e
      end
      e.should be_nil
    end
      response.should_not be_nil
      response.should == '{"Code":"BucketNotFound","Message":"No Such Bucket"}'
  end

  it "should be able to create object against an existing bucket" do

    response = nil
    EM.run do
      begin
        EM.add_timer(0) { `curl http://127.0.0.1:#{@resp['port']}/blob_unit_test/testfile1 --request PUT -T testfile1.txt -s`}
        EM.add_timer(1) { response = `curl http://127.0.0.1:#{@resp['port']}/blob_unit_test/testfile1 -o testfile1_upload.txt -s`
                          response = `diff testfile1.txt testfile1_upload.txt`
                        }
        EM.add_timer(2) { EM.stop }
      rescue Exception => e
        @logger.debug e
      end
      e.should be_nil
    end
      response.should_not be_nil
      response.should == ''
  end

  it "should be able to alert when deleting a non-existing object" do
    response = nil
    EM.run do
      begin
        EM.add_timer(0) { response = `curl http://127.0.0.1:#{@resp['port']}/blob_unit_test/testfile2 --request DELETE -s`}
        EM.add_timer(1) { EM.stop }
      rescue Exception => e
        @logger.debug e
      end
      e.should be_nil
    end
      response.should_not be_nil
      response.should == '{"Error":{"Code":"NoSuchFile","Message":"File does not exists on Disk"}}'
  end

  it "should be able to alert when deleting a non-empty bucket" do
    response = nil
    EM.run do
      begin
        EM.add_timer(0) { response = `curl http://127.0.0.1:#{@resp['port']}/blob_unit_test --request DELETE -s`}
        EM.add_timer(1) { EM.stop }
      rescue Exception => e
        @logger.debug e
      end
      e.should be_nil
    end
      response.should_not be_nil
      response.should == '{"Error":{"Code":"BucketNotEmpty","Message":"The bucket you tried to delete is not empty."}}'
  end

  it "should be able to delete an existing object" do
    response = nil
    EM.run do
      begin
        EM.add_timer(5) { response = `curl http://127.0.0.1:#{@resp['port']}/blob_unit_test/testfile1 --request DELETE -s`}
        EM.add_timer(6) { EM.stop }
      rescue Exception => e
        @logger.debug e
      end
      e.should be_nil
    end
      response.should_not be_nil
      response.should == ''
  end

  it "should keep the result after node restart" do
    port_open_1 = nil
    port_open_2 = nil
    meta_port_open_1 = nil
    meta_port_open_2 = nil
    EM.run do
      EM.add_timer(0) { @node.shutdown }
      EM.add_timer(1) { port_open_1 = is_port_open?('127.0.0.1', @resp['port'])
                        meta_port_open_1 = is_port_open?('127.0.0.1',@resp['meta_port'])
                      }
      EM.add_timer(2) { @node = Node.new(@opts) }
      EM.add_timer(3) { port_open_2 = is_port_open?('127.0.0.1', @resp['port'])
                        meta_port_open_2 = is_port_open?('127.0.0.1',@resp['meta_port'])
                      }
      EM.add_timer(4) { EM.stop }
    end

    port_open_1.should be_false
    meta_port_open_1.should be_false
    port_open_2.should be_true
    meta_port_open_2.should be_true
    response = `curl http://127.0.0.1:#{@resp['port']}/blob_unit_test/ -s`
    response.should_not be_nil
    response.should == '{"ListBucketResult":{"Name":"blob_unit_test","Prefix":{},"Marker":{},"MaxKeys":"1000","IsTruncated":"false"}}'
  end

  it "should be able to delete an empty bucket" do
    response = nil
    EM.run do
      begin
        EM.add_timer(0) { response = `curl http://127.0.0.1:#{@resp['port']}/blob_unit_test --request DELETE -s`}
        EM.add_timer(1) { EM.stop }
      rescue Exception => e
        @logger.debug e
      end
      e.should be_nil
    end
      response.should_not be_nil
      response.should == ''
  end

  it "should return error when unprovisioning a non-existed instance" do
    EM.run do
      e = nil
      begin
        @node.unprovision('no existed', [])
      rescue => e
      end
      e.should_not be_nil
      EM.stop
    end
  end

  it "should report error when admin users are deleted from mongodb" do
    EM.run do
      delete_admin(@resp)
      stats = @node.varz_details
      stats.should_not be_nil
      stats[:running_services].length.should > 0
      stats[:running_services][0]['db'].class.should == String
      stats[:running_services][0]['overall'].class.should == String
      EM.stop
    end
  end

  # unprovision here
  it "should be able to unprovision an existing instance" do
    EM.run do
      @node.unprovision(@resp['name'], [])

      e = nil
      begin
        conn = Mongo::Connection.new('localhost', @resp['meta_port']).db('db')
      rescue => e
      end
      e.should_not be_nil
      EM.stop
    end
  end

  it "should release memory" do
    EM.run do
      @original_memory.should == @node.available_memory
      EM.stop
    end
  end

end


