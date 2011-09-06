# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), '..')
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'rubygems'
require 'rspec'
require 'logger'

require 'base/base'
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

TEST_CHECK_INS_LIST = ["id_for_normal_ins","id_for_orphan_ins"]
TEST_CHECK_BIND_LIST =
[
  {
    "name"     => "id_for_normal_ins",
    "hostname" => "127.0.0.1",
    "host"     => "127.0.0.1",
    "port"     => 3306,
    "user"     => "username_for_normal_binding",
    "username" => "username_for_normal_binding",
    "password" => "pl9bMF25hwtlS"
  },
  {
    "name"     => "id_for_unknown_ins",
    "hostname" => "127.0.0.1",
    "host"     => "127.0.0.1",
    "port"     => 3306,
    "user"     => "username_for_orphan_binding",
    "username" => "username_for_orphan_binding",
    "password" => "pUFwntBqZojMo"
  }
]

TEST_CHECK_HANDLES =
[
  {
    "service_id"    => "id_for_normal_ins",
    "configuration" => {
      "plan" => "free"
    },
    "credentials"   => {
      "name"     => "id_for_normal_ins",
      "hostname" => "127.0.0.1",
      "host"     => "127.0.0.1",
      "port"     => 3306,
      "user"     => "umLImcKDtRtID",
      "username" => "umLImcKDtRtID",
      "password" => "p1ZivmGDSJXSC",
      "node_id"  => "#{TEST_NODE_ID}"
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
      "node_id"  => "#{TEST_NODE_ID+'2'}"
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
      "name"     => "id_for_normal_ins",
      "hostname" => "127.0.0.1",
      "host"     => "127.0.0.1",
      "port"     => 3306,
      "user"     => "username_for_normal_binding",
      "username" => "username_for_normal_binding",
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

class BaseTests

  module Options

    LOGGER = Logger.new(STDOUT)

    NATS_URI = "nats://localhost:4222"

    IP_ROUTE = "127.0.0.1"

    def self.default(more=nil)
      options = {
        :logger => LOGGER,
        :ip_route => IP_ROUTE,
        :mbus => NATS_URI,
      }
      more.each { |k,v| options[k] = v } if more
      options
    end

  end

  def self.create_base
    BaseTester.new(Options.default)
  end

  class BaseTester < VCAP::Services::Base::Base
    attr_accessor :node_mbus_connected
    attr_accessor :varz_invoked
    attr_accessor :healthz_invoked
    def initialize(options)
      @node_mbus_connected = false
      @varz_invoked = false
      @healthz_invoked = false
      super(options)
    end
    def flavor
      "flavor"
    end
    def service_name
      "service_name"
    end
    def on_connect_node
      @node_mbus_connected = true
    end
    def varz_details
      @varz_invoked = true
      {}
    end
    def healthz_details
      @healthz_invoked = true
      {}
    end
  end

end

require 'base/node'

class NodeTests

  def self.create_node
    NodeTester.new(BaseTests::Options.default({:node_id => NodeTester::ID}))
  end

  def self.create_error_node
    NodeErrorTester.new(BaseTests::Options.default({:node_id => NodeTester::ID}))
  end

  def self.create_check_orphan_empty_node
    NodeCheckOrphanEmpty.new(BaseTests::Options.default({:node_id => TEST_NODE_ID}))
  end

  def self.create_check_orphan_full_node
    NodeCheckOrphanFull.new(BaseTests::Options.default({:node_id => TEST_NODE_ID}))
  end


  def self.create_error_provisioner
    MockErrorProvisioner.new
  end

  def self.create_provisioner
    MockProvisioner.new
  end

  class NodeTester < VCAP::Services::Base::Node
    attr_accessor :announcement_invoked
    attr_accessor :provision_invoked
    attr_accessor :unprovision_invoked
    attr_accessor :bind_invoked
    attr_accessor :unbind_invoked
    attr_accessor :restore_invoked
    attr_accessor :provision_times
    attr_reader :unprovision_count
    attr_reader :unbind_count
    SERVICE_NAME = "Test"
    ID = "node-1"
    def initialize(options)
      super(options)
      @announcement_invoked = false
      @provision_invoked = false
      @unprovision_invoked = false
      @bind_invoked = false
      @unbind_invoked = false
      @restore_invoked = false
      @provision_times = 0
      @mutex = Mutex.new
      @unprovision_count = 0
      @unbind_count = 0
    end
    def service_name
      SERVICE_NAME
    end
    def announcement
      @announcement_invoked = true
      Hash.new
    end
    def provision(plan, credential)
      sleep 5 # Provision takes 5 seconds to finish
      @mutex.synchronize { @provision_times += 1 }
      @provision_invoked = true
      Hash.new
    end
    def unprovision(name, bindings)
      @unprovision_invoked = true
      @mutex.synchronize{ @unprovision_count += 1 }
    end
    def bind(name, bind_opts, credential)
      @bind_invoked = true
    end
    def unbind(credentials)
      @unbind_invoked = true
      @mutex.synchronize{ @unbind_count += 1 }
    end
    def restore(isntance_id, backup_path)
      @restore_invoked = true
    end
  end

  NodeCheckOrphanEmpty = NodeTester

  class NodeCheckOrphanFull < NodeCheckOrphanEmpty
    def initialize(options)
      super(options)
    end

    def all_instances_list
      TEST_CHECK_INS_LIST
    end

    def all_bindings_list
      TEST_CHECK_BIND_LIST
    end
  end

  class MockProvisioner
    include VCAP::Services::Internal
    attr_accessor :got_announcement
    attr_accessor :got_provision_response
    attr_accessor :got_check_orphan_response
    attr_accessor :got_purge_orphan_response
    attr_accessor :orphan_ins_hash
    attr_accessor :orphan_binding_hash
    def initialize
      @got_announcement = false
      @got_provision_response = false
      @got_unprovision_response = false
      @got_bind_response = false
      @got_unbind_response = false
      @got_restore_response = false
      @got_check_orphan_response = false
      @got_purge_orphan_response = false
      @orphan_ins_hash = {}
      @orphan_binding_hash = {}
      @nats = NATS.connect(:uri => BaseTests::Options::NATS_URI) {
        @nats.subscribe("#{NodeTester::SERVICE_NAME}.announce") {
          @got_announcement = true
        }
        @nats.subscribe("#{NodeTester::SERVICE_NAME}.orphan_result") do |msg|
          response = CheckOrphanResponse.decode(msg)
          @orphan_ins_hash.merge!(response.orphan_instances)
          @orphan_binding_hash.merge!(response.orphan_bindings)
          @got_check_orphan_response = true
        end

        @nats.publish("#{NodeTester::SERVICE_NAME}.discover")
      }
    end
    def send_provision_request
      req = ProvisionRequest.new
      req.plan = "free"
      @nats.request("#{NodeTester::SERVICE_NAME}.provision.#{NodeTester::ID}", req.encode) {
        @got_provision_response = true
      }
    end
    def send_unprovision_request
      req = UnprovisionRequest.new
      req.name = "fake"
      req.bindings = []
      @nats.request("#{NodeTester::SERVICE_NAME}.unprovision.#{NodeTester::ID}", req.encode ) {
        @got_unprovision_response = true
      }
    end
    def send_bind_request
      req = BindRequest.new
      req.name = "fake"
      req.bind_opts = {}
      @nats.request("#{NodeTester::SERVICE_NAME}.bind.#{NodeTester::ID}", req.encode) {
        @got_bind_response = true
      }
    end
    def send_unbind_request
      req = UnbindRequest.new
      req.credentials = {}
      @nats.request("#{NodeTester::SERVICE_NAME}.unbind.#{NodeTester::ID}", req.encode) {
        @got_unbind_response = true
      }
    end
    def send_restore_request
      req = RestoreRequest.new
      req.instance_id = "fake1"
      req.backup_path = "/tmp"
      @nats.request("#{NodeTester::SERVICE_NAME}.restore.#{NodeTester::ID}", req.encode) {
        @got_restore_response = true
      }
    end
    def send_check_orphan_request
      req = CheckOrphanRequest.new
      req.handles = TEST_CHECK_HANDLES
      @nats.publish("#{NodeTester::SERVICE_NAME}.check_orphan", req.encode)
    end
    def send_purge_orphan_request
      req = PurgeOrphanRequest.new
      req.orphan_ins_list = TEST_PURGE_INS_HASH[TEST_NODE_ID]
      req.orphan_binding_list = TEST_PURGE_BIND_HASH[TEST_NODE_ID]
      @nats.request("#{NodeTester::SERVICE_NAME}.purge_orphan.#{NodeTester::ID}", req.encode) {
        @got_purge_orphan_response = true
      }
    end
  end

  # Test Node which raise error
  class NodeErrorTester < VCAP::Services::Base::Node
    include VCAP::Services::Base::Error
    attr_accessor :announcement_invoked
    attr_accessor :provision_invoked
    attr_accessor :unprovision_invoked
    attr_accessor :bind_invoked
    attr_accessor :unbind_invoked
    attr_accessor :restore_invoked
    attr_accessor :check_orphan_invoked
    attr_accessor :purge_orphan_invoked
    attr_accessor :provision_times
    SERVICE_NAME = "Test"
    ID = "node-error"
    def initialize(options)
      super(options)
      @announcement_invoked = false
      @provision_invoked = false
      @unprovision_invoked = false
      @bind_invoked = false
      @unbind_invoked = false
      @restore_invoked = false
      @check_orphan_invoked = false
      @purge_orphan_invoked = false
      @provision_times = 0
      @mutex = Mutex.new
    end
    def service_name
      SERVICE_NAME
    end
    def announcement
      @announcement_invoked = true
      Hash.new
    end
    def provision(plan, credential)
      @provision_invoked = true
      raise ServiceError.new(ServiceError::SERVICE_UNAVAILABLE)
    end
    def unprovision(name, bindings)
      @unprovision_invoked = true
      raise ServiceError.new(ServiceError::SERVICE_UNAVAILABLE)
    end
    def bind(name, bind_opts, credential)
      @bind_invoked = true
      raise ServiceError.new(ServiceError::SERVICE_UNAVAILABLE)
    end
    def unbind(credentials)
      @unbind_invoked = true
      raise ServiceError.new(ServiceError::SERVICE_UNAVAILABLE)
    end
    def restore(isntance_id, backup_path)
      @restore_invoked = true
      raise ServiceError.new(ServiceError::SERVICE_UNAVAILABLE)
    end
    def check_orphan(handles)
      @check_orphan_invoked = true
      raise ServiceError.new(ServiceError::SERVICE_UNAVAILABLE)
    end
    def purge_orphan(oi_list,ob_list)
      @purge_orphan_invoked = true
      raise ServiceError.new(ServiceError::SERVICE_UNAVAILABLE)
    end
  end

  # Provisioner that catch error from node
  class MockErrorProvisioner < MockProvisioner
    include VCAP::Services::Internal
    attr_accessor :got_announcement
    attr_accessor :got_provision_response
    attr_accessor :got_unprovision_response
    attr_accessor :got_bind_response
    attr_accessor :got_unbind_response
    attr_accessor :got_restore_response
    attr_accessor :got_check_orphan_response
    attr_accessor :response
    def initialize
      @got_announcement = false
      @got_provision_response = false
      @got_unprovision_response = false
      @got_bind_response = false
      @got_unbind_response = false
      @got_restore_response = false
      @got_check_orphan_response = false
      @nats = NATS.connect(:uri => BaseTests::Options::NATS_URI) {
        @nats.subscribe("#{NodeTester::SERVICE_NAME}.announce") {
          @got_announcement = true
        }
        @nats.subscribe("#{NodeTester::SERVICE_NAME}.orphan_result") do |msg|
          res = CheckOrphanResponse.decode(msg)
          @response = res.error
          @got_check_orphan_response = true
        end
        @nats.publish("#{NodeTester::SERVICE_NAME}.discover")
      }
      @response = nil
    end
    def send_provision_request
      req = ProvisionRequest.new
      req.plan = "free"
      @nats.request("#{NodeTester::SERVICE_NAME}.provision.#{NodeTester::ID}", req.encode) do |msg|
        @got_provision_response = true
        @response = msg
      end
    end
    def send_unprovision_request
      req = UnprovisionRequest.new
      req.name = "fake"
      req.bindings = []
      @nats.request("#{NodeTester::SERVICE_NAME}.unprovision.#{NodeTester::ID}", req.encode ) do |msg|
        @got_unprovision_response = true
        @response = msg
      end
    end
    def send_bind_request
      req = BindRequest.new
      req.name = "fake"
      req.bind_opts = {}
      @nats.request("#{NodeTester::SERVICE_NAME}.bind.#{NodeTester::ID}", req.encode) do |msg|
        @got_bind_response = true
        @response = msg
      end
    end
    def send_unbind_request
      req = UnbindRequest.new
      req.credentials = {}
      @nats.request("#{NodeTester::SERVICE_NAME}.unbind.#{NodeTester::ID}", req.encode) do |msg|
        @got_unbind_response = true
        @response = msg
      end
    end
    def send_restore_request
      req = RestoreRequest.new
      req.instance_id = "fake1"
      req.backup_path = "/tmp"
      @nats.request("#{NodeTester::SERVICE_NAME}.restore.#{NodeTester::ID}", req.encode) do |msg|
        @got_restore_response = true
        @response = msg
      end
    end
    def send_check_orphan_request
      req = CheckOrphanRequest.new
      req.handles = TEST_CHECK_HANDLES
      @nats.publish("#{NodeTester::SERVICE_NAME}.check_orphan", req.encode)
    end
    def send_purge_orphan_request
      req = PurgeOrphanRequest.new
      req.orphan_ins_list = TEST_PURGE_INS_HASH[TEST_NODE_ID]
      req.orphan_binding_list = TEST_PURGE_BIND_HASH[TEST_NODE_ID]
      @nats.request("#{NodeTester::SERVICE_NAME}.purge_orphan.#{NodeTester::ID}", req.encode) do |msg|
        @got_unbind_response = true
        @response = msg
      end
    end
  end
end

require 'base/provisioner'

class ProvisionerTests

  def self.create_provisioner(options = {})
    ProvisionerTester.new(BaseTests::Options.default(options))
  end

  def self.create_gateway(provisioner)
    MockGateway.new(provisioner)
  end

  def self.create_error_gateway(provisioner)
    MockErrorGateway.new(provisioner)
  end

  def self.create_node(id, score = 1)
    MockNode.new(id, score)
  end

  def self.create_error_node(id, score = 1)
    MockErrorNode.new(id, score)
  end

  def self.setup_fake_instance(gateway, provisioner, node)
    instance_id = "fake_instance"
    gateway.instance_id = instance_id
    provisioner.prov_svcs[instance_id] = {:credentials => {'node_id' =>node.node_id }}
  end

  class ProvisionerTester < VCAP::Services::Base::Provisioner
    attr_accessor :prov_svcs
    attr_accessor :varz_invoked
    attr_accessor :healthz_invoked
    attr_accessor :prov_svcs
    attr_reader   :check_orphan_invoked
    def initialize(options)
      super(options)
      @varz_invoked = false
      @healthz_invoked = false
      @check_orphan_invoked = false
    end
    SERVICE_NAME = "Test"
    def service_name
      SERVICE_NAME
    end
    def node_score(node)
      node["score"]
    end
    def node_count
      return @nodes.length
    end
    def varz_details
      @varz_invoked = true
      super
    end
    def healthz_details
      @healthz_invoked = true
      super
    end
    def on_orphan_result(msg)
      @check_orphan_invoked = true
      super(msg)
    end
  end

  class MockGateway
    attr_accessor :got_announcement
    attr_accessor :got_provision_response
    attr_accessor :got_unprovision_response
    attr_accessor :got_bind_response
    attr_accessor :got_unbind_response
    attr_accessor :got_restore_response
    attr_accessor :got_recover_response
    attr_reader :got_purge_orphan_response
    attr_reader :got_check_orphan_response
    def initialize(provisioner)
      @provisioner = provisioner
      @got_announcement = false
      @got_provision_response = false
      @got_unprovision_response = false
      @got_bind_response = false
      @got_unbind_response = false
      @got_restore_response = false
      @got_recover_response = false
      @got_purge_orphan_response = false
      @got_check_orphan_response = false
      @instance_id = nil
      @bind_id = nil
    end
    def send_provision_request
      req = {'plan' => 'free'}
      @provisioner.provision_service(req, nil) do |res|
        @instance_id = res['response'][:service_id]
        @got_provision_response = res['success']
      end
    end
    def send_unprovision_request
      @provisioner.unprovision_service(@instance_id) do |res|
        @got_unprovision_response = res['success']
      end
    end
    def send_bind_request
      @provisioner.bind_instance(@instance_id, {}, nil) do |res|
        @bind_id = res['response'][:service_id]
        @got_bind_response = res['success']
      end
    end
    def send_unbind_request
      @provisioner.unbind_instance(@instance_id, @bind_id, nil) do |res|
        @got_unbind_response = res['success']
      end
    end
    def send_restore_request
      @provisioner.restore_instance(@instance_id, nil) do |res|
        @got_restore_response = res['success']
      end
    end
    def send_recover_request
      # register a fake callback to provisioner which always return true
      @provisioner.register_update_handle_callback{|handle, &blk| blk.call(true)}
      @provisioner.recover(@instance_id, "/tmp", [{'service_id' => @instance_id, 'configuration' => {'plan' => 'free'}},{'service_id' => 'fake_uuid', 'configuration' => {}, 'credentials' => {'name' => @instance_id}}]) do |res|
        @got_recover_response = res['success']
      end
    end
    def send_check_orphan_request
      @provisioner.check_orphan(TEST_CHECK_HANDLES) do |res|
        @got_check_orphan_response = res["success"]
      end
    end
    def send_purge_orphan_request
      @provisioner.purge_orphan(TEST_PURGE_INS_HASH,TEST_PURGE_BIND_HASH) do |res|
        @got_purge_orphan_response = res['success']
      end
    end
  end

  # Gateway that catch error from node
  class MockErrorGateway < MockGateway
    attr_accessor :got_announcement
    attr_accessor :provision_response
    attr_accessor :unprovision_response
    attr_accessor :bind_response
    attr_accessor :unbind_response
    attr_accessor :restore_response
    attr_accessor :recover_response
    attr_accessor :error_msg
    attr_accessor :instance_id
    attr_accessor :bind_id
    def initialize(provisioner)
      @provisioner = provisioner
      @got_announcement = false
      @provision_response = true
      @unprovision_response = true
      @bind_response = true
      @unbind_response = true
      @restore_response = true
      @recover_response = true
      @error_msg = nil
      @instance_id = nil
      @bind_id = nil
    end
    def send_provision_request
      req = {'plan' => 'free'}
      @provisioner.provision_service(req, nil) do |res|
        @provision_response = res['success']
        @error_msg = res['response']
      end
    end
    def send_unprovision_request
      @provisioner.unprovision_service(@instance_id) do |res|
        @unprovision_response = res['success']
        @error_msg = res['response']
      end
    end
    def send_bind_request
      @provisioner.bind_instance(@instance_id, {}, nil) do |res|
        @bind_response = res['success']
        @bind_id = res['response'][:service_id]
        @bind_response = res['success']
        @error_msg = res['response']
      end
    end
    def send_unbind_request
      @provisioner.unbind_instance(@instance_id, @bind_id, nil) do |res|
        @unbind_response = res['success']
        @error_msg = res['response']
      end
    end
    def send_restore_request
      @provisioner.restore_instance(@instance_id, nil) do |res|
        @restore_response = res['success']
        @error_msg = res['response']
      end
    end
    def send_recover_request
      # register a fake callback to provisioner which always return true
      @provisioner.register_update_handle_callback{|handle, &blk| blk.call(true)}
      @provisioner.recover(@instance_id, "/tmp", [{'service_id' => @instance_id, 'configuration' => {'plan' => 'free'}},{'service_id' => 'fake_uuid', 'configuration' => {}, 'credentials' => {'name' => @instance_id}}]) do |res|
        @recover_response = res['success']
        @error_msg = res['response']
      end
    end
    def send_purge_orphan_request
      @provisioner.purge_orphan(TEST_PURGE_INS_HASH,TEST_PURGE_BIND_HASH) do |res|
        @error_msg = res['response']
      end
    end
  end

  class MockNode
    include VCAP::Services::Internal
    attr_accessor :got_unprovision_request
    attr_accessor :got_provision_request
    attr_accessor :got_unbind_request
    attr_accessor :got_bind_request
    attr_accessor :got_restore_request
    attr_reader :got_check_orphan_request
    attr_reader :got_purge_orphan_request
    def initialize(id, score)
      @id = id
      @score = score
      @got_provision_request = false
      @got_unprovision_request = false
      @got_bind_request = false
      @got_unbind_request = false
      @got_restore_request = false
      @got_check_orphan_request = false
      @got_purge_orphan_request = false
      @nats = NATS.connect(:uri => BaseTests::Options::NATS_URI) {
        @nats.subscribe("#{service_name}.discover") { |_, reply|
          announce(reply)
        }
        @nats.subscribe("#{service_name}.provision.#{node_id}") { |_, reply|
          @got_provision_request = true
          response = ProvisionResponse.new
          response.success = true
          response.credentials = {
              'name' => UUIDTools::UUID.random_create.to_s,
              'node_id' => node_id,
              'username' => UUIDTools::UUID.random_create.to_s,
              'password' => UUIDTools::UUID.random_create.to_s,
            }
          @nats.publish(reply, response.encode)
        }
        @nats.subscribe("#{service_name}.unprovision.#{node_id}") { |msg, reply|
          @got_unprovision_request = true
          response = SimpleResponse.new
          response.success = true
          @nats.publish(reply, response.encode)
        }
        @nats.subscribe("#{service_name}.bind.#{node_id}") { |msg, reply|
          @got_bind_request = true
          response = BindResponse.new
          response.success = true
          response.credentials = {
              'name' => UUIDTools::UUID.random_create.to_s,
              'node_id' => node_id,
              'username' => UUIDTools::UUID.random_create.to_s,
              'password' => UUIDTools::UUID.random_create.to_s,
            }
          @nats.publish(reply, response.encode)
        }
        @nats.subscribe("#{service_name}.unbind.#{node_id}") { |msg, reply|
          @got_unbind_request = true
          response = SimpleResponse.new
          response.success = true
          @nats.publish(reply, response.encode)
        }
        @nats.subscribe("#{service_name}.restore.#{node_id}") { |msg, reply|
          @got_restore_request = true
          response = SimpleResponse.new
          response.success = true
          @nats.publish(reply, response.encode)
        }
        @nats.subscribe("#{service_name}.check_orphan") do |msg, reply|
          @got_check_orphan_request = true
          response = CheckOrphanResponse.new
          response.orphan_instances = {}
          response.orphan_bindings = {}
          response.orphan_instances["#{node_id}"] = (1..@id).to_a
          response.orphan_bindings["#{node_id}"] = (1..@id).to_a
          response.success = true
          @nats.publish("#{service_name}.orphan_result", response.encode)
        end
        @nats.subscribe("#{service_name}.purge_orphan.#{node_id}") do |msg, reply|
          @got_purge_orphan_request = true
          response = SimpleResponse.new
          response.success = true
          @nats.publish(reply,response.encode)
        end
        announce
      }
    end
    def service_name
      ProvisionerTester::SERVICE_NAME
    end
    def node_id
      "node-#{@id}"
    end
    def announce(reply=nil)
      a = { :id => node_id, :score => @score }
      @nats.publish(reply||"#{service_name}.announce", a.to_json)
    end
  end

  # The node that generates error response
  class MockErrorNode < MockNode
    include VCAP::Services::Base::Error
    attr_accessor :got_unprovision_request
    attr_accessor :got_provision_request
    attr_accessor :got_unbind_request
    attr_accessor :got_bind_request
    attr_accessor :got_restore_request
    def initialize(id, score)
      @id = id
      @score = score
      @got_provision_request = false
      @got_unprovision_request = false
      @got_bind_request = false
      @got_unbind_request = false
      @got_restore_request = false
      @got_check_orphan_request = false
      @got_purge_orphan_request = false
      @internal_error = ServiceError.new(ServiceError::INTERNAL_ERROR)
      @nats = NATS.connect(:uri => BaseTests::Options::NATS_URI) {
        @nats.subscribe("#{service_name}.discover") { |_, reply|
          announce(reply)
        }
        @nats.subscribe("#{service_name}.provision.#{node_id}") { |_, reply|
          @got_provision_request = true
          response = ProvisionResponse.new
          response.success = false
          response.error = @internal_error.to_hash
          @nats.publish(reply, response.encode)
        }
        @nats.subscribe("#{service_name}.unprovision.#{node_id}") { |msg, reply|
          @got_unprovision_request = true
          @nats.publish(reply, gen_simple_error_response.encode)
        }
        @nats.subscribe("#{service_name}.bind.#{node_id}") { |msg, reply|
          @got_bind_request = true
          response = BindResponse.new
          response.success = false
          response.error = @internal_error.to_hash
          @nats.publish(reply, response.encode)
        }
        @nats.subscribe("#{service_name}.unbind.#{node_id}") { |msg, reply|
          @got_unbind_request = true
          @nats.publish(reply, gen_simple_error_response.encode)
        }
        @nats.subscribe("#{service_name}.restore.#{node_id}") { |msg, reply|
          @got_restore_request = true
          @nats.publish(reply, gen_simple_error_response.encode)
        }
        @nats.subscribe("#{service_name}.check_orphan") do |msg, reply|
          @got_check_orphan_request = true
          response = CheckOrphanResponse.new
          response.success = false
          response.error = @internal_error.to_hash
          @nats.publish("#{service_name}.orphan_result", response.encode)
        end
        @nats.subscribe("#{service_name}.purge_orphan.#{node_id}") do |msg, reply|
          @got_purge_orphan_request = true
          @nats.publish(reply,gen_simple_error_response.encode)
        end
        announce
      }
    end

    def gen_simple_error_response
      res = SimpleResponse.new
      res.success = false
      res.error = @internal_error.to_hash
      res
    end
  end

end

require 'base/asynchronous_service_gateway'

class AsyncGatewayTests
  CC_PORT = 34512
  GW_PORT = 34513

  def self.create_nice_gateway
    MockGateway.new(true)
  end

  def self.create_nasty_gateway
    MockGateway.new(false)
  end

  def self.create_cloudcontroller
    MockCloudController.new
  end

  class MockGateway
    attr_accessor :provision_http_code
    attr_accessor :unprovision_http_code
    attr_accessor :bind_http_code
    attr_accessor :unbind_http_code
    attr_accessor :restore_http_code
    attr_accessor :recover_http_code
    attr_reader   :purge_orphan_http_code
    attr_reader   :check_orphan_http_code

    def initialize(nice)
      @token = '0xdeadbeef'
      @cc_head = {
        'Content-Type'         => 'application/json',
        'X-VCAP-Service-Token' => @token,
      }
      @label = "service-1.0"
      @sp = nice ? NiceProvisioner.new : NastyProvisioner.new
      sg = VCAP::Services::AsynchronousServiceGateway.new(
        :service => {
                      :label => @label,
                      :name => 'service',
                      :version => '1.0',
                      :description => 'sample desc',
                      :plans => ['free'],
                      :tags => ['nosql']
                    },
        :token   => @token,
        :provisioner => @sp,
        :cloud_controller_uri => "http://localhost:#{CC_PORT}",
        :check_orphan_interval => 5
      )
      @server = Thin::Server.new('localhost', GW_PORT, sg)
      @provision_http_code = 0
      @unprovision_http_code = 0
      @bind_http_code = 0
      @unbind_http_code = 0
      @restore_http_code = 0
      @recover_http_code = 0
      @purge_orphan_http_code = 0
      @check_orphan_http_code = 0
      @last_service_id = nil
      @last_bind_id = nil
    end

    def start
      Thread.new { @server.start }
    end

    def stop
      @server.stop
    end

    def gen_req(body = nil)
      req = { :head => @cc_head }
      req[:body] = body if body
      req
    end

    def check_orphan_invoked
      @sp.check_orphan_invoked
    end

    def send_provision_request
      msg = Yajl::Encoder.encode({
        :label => @label,
        :plan  => "free"
      })
      http = EM::HttpRequest.new("http://localhost:#{GW_PORT}/gateway/v1/configurations").post(gen_req(msg))
      http.callback {
        @provision_http_code = http.response_header.status
        res = Yajl::Parser.parse(http.response)
        @last_service_id = res['service_id']
      }
      http.errback {
        @provision_http_code = -1
      }
    end

    def send_unprovision_request(service_id = nil)
      service_id ||= @last_service_id
      http = EM::HttpRequest.new("http://localhost:#{GW_PORT}/gateway/v1/configurations/#{service_id}").delete(gen_req)
      http.callback {
        @unprovision_http_code = http.response_header.status
      }
      http.errback {
        @unprovision_http_code = -1
      }
    end

    def send_bind_request(service_id = nil)
      service_id ||= @last_service_id
      msg = Yajl::Encoder.encode({
        :service_id => service_id,
        :label => @label,
        :binding_options => {}
      })
      http = EM::HttpRequest.new("http://localhost:#{GW_PORT}/gateway/v1/configurations/#{service_id}/handles").post(gen_req(msg))
      http.callback {
        @bind_http_code = http.response_header.status
        res = Yajl::Parser.parse(http.response)
        @last_bind_id = res['service_id']
      }
      http.errback {
        @bind_http_code = -1
      }
    end

    def send_unbind_request(service_id = nil, bind_id = nil)
      service_id ||= @last_service_id
      bind_id ||= @last_bind_id
      msg = Yajl::Encoder.encode({
        :service_id => service_id,
        :handle_id => bind_id,
        :binding_options => {}
      })
      http = EM::HttpRequest.new("http://localhost:#{GW_PORT}/gateway/v1/configurations/#{service_id}/handles/#{bind_id}").delete(gen_req(msg))
      http.callback {
        @unbind_http_code = http.response_header.status
      }
      http.errback {
        @unbind_http_code = -1
      }
    end

    def send_restore_request(service_id = nil)
      service_id ||= @last_service_id
      msg = Yajl::Encoder.encode({
        :instance_id => service_id,
        :backup_path => '/'
      })
      http = EM::HttpRequest.new("http://localhost:#{GW_PORT}/service/internal/v1/restore").post(gen_req(msg))
      http.callback {
        @restore_http_code = http.response_header.status
      }
      http.errback {
        @restore_http_code = -1
      }
    end

    def send_recover_request(service_id = nil)
      service_id ||= @last_service_id
      msg = Yajl::Encoder.encode({
        :instance_id => service_id,
        :backup_path => '/'
      })
      http = EM::HttpRequest.new("http://localhost:#{GW_PORT}/service/internal/v1/recover").post(gen_req(msg))
      http.callback {
        @recover_http_code = http.response_header.status
      }
      http.errback {
        @recover_http_code = -1
      }
    end

    def send_purge_orphan_request
      msg = Yajl::Encoder.encode({
        :orphan_instances => TEST_PURGE_INS_HASH,
        :orphan_bindings => TEST_PURGE_BIND_HASH
      })
      http = EM::HttpRequest.new("http://localhost:#{GW_PORT}/service/internal/v1/purge_orphan").delete(gen_req(msg))
      http.callback {
        @purge_orphan_http_code = http.response_header.status
      }
      http.errback {
        @purge_orphan_http_code = -1
      }
    end
    def send_check_orphan_request
      msg = Yajl::Encoder.encode({
      })
      http = EM::HttpRequest.new("http://localhost:#{GW_PORT}/service/internal/v1/check_orphan").post(gen_req(msg))
      http.callback {
        @check_orphan_http_code = http.response_header.status
      }
      http.errback {
        @check_orphan_http_code = -1
      }
    end
  end

  class MockCloudController
    def initialize
      @server = Thin::Server.new('localhost', CC_PORT, Handler.new)
    end

    def start
      Thread.new { @server.start }
    end

    def stop
      @server.stop if @server
    end

    class Handler < Sinatra::Base
      post "/services/v1/offerings" do
        "{}"
      end

      get "/services/v1/offerings/:label/handles" do
        Yajl::Encoder.encode({
          :handles => [{
            'service_id' => MockProvisioner::SERV_ID,
            'configuration' => {},
            'credentials' => {}
          }]
        })
      end

      get "/services/v1/offerings/:label/handles/:id" do
        "{}"
      end
    end
  end

  class MockProvisioner
    SERV_ID = "service_id"
    BIND_ID = "bind_id"

    include VCAP::Services::Base::Error

    attr_accessor :got_provision_request
    attr_accessor :got_unprovision_request
    attr_accessor :got_bind_request
    attr_accessor :got_unbind_request
    attr_accessor :got_restore_request
    attr_accessor :got_recover_request
    attr_reader   :purge_orphan_invoked
    attr_reader   :check_orphan_invoked

    def initialize
      @got_provision_request = false
      @got_unprovision_request = false
      @got_bind_request = false
      @got_unbind_request = false
      @got_restore_request = false
      @got_recover_request = false
      @purge_orphan_invoked = false
      @check_orphan_invoked = false
    end

    def register_update_handle_callback
      # Do nothing
    end

    def update_handles(handles)
      # Do nothing
    end

  end

  class NiceProvisioner < MockProvisioner
    def provision_service(request, prov_handle=nil, &blk)
      @got_provision_request = true
      blk.call(success({:data => {}, :service_id => SERV_ID, :credentials => {}}))
    end

    def unprovision_service(instance_id, &blk)
      @got_unprovision_request = true
      blk.call(success(true))
    end

    def bind_instance(instance_id, binding_options, bind_handle=nil, &blk)
      @got_bind_request = true
      blk.call(success({:configuration => {}, :service_id => BIND_ID, :credentials => {}}))
    end

    def unbind_instance(instance_id, handle_id, binding_options, &blk)
      @got_unbind_request = true
      blk.call(success(true))
    end

    def restore_instance(instance_id, backup_path, &blk)
      @got_restore_request = true
      blk.call(success(true))
    end

    def recover(instance_id, backup_path, handles, &blk)
      @got_recover_reqeust = true
      blk.call(success(true))
    end


    def purge_orphan(orphan_ins_hash,orphan_binding_hash,&blk)
      @purge_orphan_invoked = true
      blk.call(success(true))
    end

    def check_orphan(handles,&blk)
      @check_orphan_invoked = true
      blk.call(success(true))
    end
  end

  class NastyProvisioner < MockProvisioner
    def provision_service(request, prov_handle=nil, &blk)
      @got_provision_request = true
      blk.call(internal_fail)
    end

    def unprovision_service(instance_id, &blk)
      @got_unprovision_request = true
      blk.call(internal_fail)
    end

    def bind_instance(instance_id, binding_options, bind_handle=nil, &blk)
      @got_bind_request = true
      blk.call(internal_fail)
    end

    def unbind_instance(instance_id, handle_id, binding_options, &blk)
      @got_unbind_request = true
      blk.call(internal_fail)
    end

    def restore_instance(instance_id, backup_path, &blk)
      @got_restore_request = true
      blk.call(internal_fail)
    end

    def recover(instance_id, backup_path, handles, &blk)
      @got_recover_reqeust = true
      blk.call(internal_fail)
    end

    def purge_orphan(orphan_ins_hash,orphan_binding_hash,&blk)
      @purge_orphan_invoked = true
      blk.call(internal_fail)
    end
    def check_orphan(handles,&blk)
      @check_orphan_invoked = true
      blk.call(internal_fail)
    end
  end
end
