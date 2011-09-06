# Copyright (c) 2009-2011 VMware, Inc.
require "pp"
require "set"
require "datamapper"
require "uuidtools"

$LOAD_PATH.unshift File.dirname(__FILE__)
require 'base/base'
require 'barrier'
require 'service_message'

class VCAP::Services::Base::Provisioner < VCAP::Services::Base::Base
  include VCAP::Services::Internal
  MASKED_PASSWORD = '********'

  def initialize(options)
    super(options)
    @version   = options[:version]
    @node_timeout = options[:node_timeout] || 2
    @allow_over_provisioning = options[:allow_over_provisioning]
    @nodes     = {}
    @prov_svcs = {}
    EM.add_periodic_timer(60) { process_nodes }
  end

  def flavor
    'Provisioner'
  end

  # Updates our internal state to match that supplied by handles
  # +handles+  An array of config handles
  def update_handles(handles)
    @logger.debug("Update handles: #{handles.inspect}")
    handles.each do |handle|
      h = handle.deep_dup
      @prov_svcs[h['service_id']] = {
        :configuration => h['configuration'],
        :credentials => h['credentials'],
        :service_id => h['service_id']
      }
    end
    @logger.debug("[#{service_description}] Handles updated prov_svcs: #{@prov_svcs}")
  end

  def find_all_bindings(name)
    res = []
    @prov_svcs.each do |k,v|
      res << v[:credentials] if v[:credentials]["name"] == name && v[:service_id] != name
    end
    res
  end

  def process_nodes
    @nodes.delete_if {|_, timestamp| Time.now.to_i - timestamp > 300}
  end

  def on_connect_node
    @logger.debug("[#{service_description}] Connected to node mbus..")
    @node_nats.subscribe("#{service_name}.announce") { |msg|
      on_node_announce(msg)
    }
    @node_nats.subscribe("#{service_name}.orphan_result") do |msg|
      on_orphan_result(msg)
    end
    @node_nats.subscribe("#{service_name}.handles") {|msg, reply| on_query_handles(msg, reply) }
    @node_nats.subscribe("#{service_name}.update_service_handle") {|msg, reply| on_update_service_handle(msg, reply) }
    @node_nats.publish("#{service_name}.discover")
  end

  def on_node_announce(msg)
    @logger.debug("[#{service_description}] Received node announcement: #{msg}")
    announce_message = Yajl::Parser.parse(msg)
    @nodes[announce_message["id"]] = Time.now.to_i if announce_message["id"]
  end

  # query all handles for a given instance
  def on_query_handles(instance, reply)
    @logger.debug("[#{service_description}] Receive query handles request for instance: #{instance}")
    if instance.empty?
      res = Yajl::Encoder.encode(@prov_svcs)
    else
      handles = find_all_bindings(msg)
      res = Yajl::Encoder.encode(handles)
    end
    @node_nats.publish(reply, res)
  end

  def on_orphan_result(msg)
    @logger.debug("[#{service_description}] Received orphan result: #{msg}")
    response = CheckOrphanResponse.decode(msg)
    if response.success
      @orphan_ins_hash.merge!(response.orphan_ins_hash)
      @orphan_binding_hash.merge!(response.orphan_binding_hash)
      oi_count = @orphan_ins_hash.values.reduce(0) {|m,v| m += v.count}
      ob_count = @orphan_binding_hash.values.reduce(0) {|m,v| m += v.count}
      @logger.debug("Orphan Instances: #{oi_count};  Orphan Bindings: #{ob_count}")
    end
  end

  def check_orphan(handles,&blk)
    @logger.debug("[#{service_description}] Check if there are orphans")
    @orphan_ins_hash = {}
    @orphan_binding_hash = {}
    req = CheckOrphanRequest.new
    req.handles = handles
    @node_nats.publish("#{service_name}.check_orphan",req.encode)
    blk.call(success)
  rescue => e
    @logger.warn(e)
    blk.call(failure(e))
  end

  def purge_orphan(orphan_ins_hash,orphan_bind_hash,&blk)
    @logger.debug("[#{service_description}] Purge orphans for given list")
    begin
      orphan_ins_hash.each do |node_id,ins_list|
        if ob_list = orphan_bind_hash[node_id]
          orphan_bind_hash.delete(node_id)
        else
          ob_list = []
        end
        send_purge_orphan_to_node(node_id,ins_list,ob_list,&blk)
      end
      orphan_bind_hash.each do |node_id,ob_list|
        send_purge_orphan_to_node(node_id,[],ob_list,&blk)
      end
    rescue => e
      @logger.warn(e)
      if e.instance_of? ServiceError
        blk.call(failure(e))
      else
        blk.call(internal_fail)
      end
    end
  end

  def send_purge_orphan_to_node(node_id, ins_list, ob_list,&blk)
    @logger.debug("[#{service_description}] Purge orphans for #{node_id} instances: #{ins_list.inspect} bindings: #{ob_list.inspect}")
    req = PurgeOrphanRequest.new
    req.orphan_ins_list = ins_list
    req.orphan_binding_list = ob_list
    subscription = nil
    timer = EM.add_timer(@node_timeout) {
      @node_nats.unsubscribe(subscription)
      blk.call(timeout_fail)
    }
    subscription =
      @node_nats.request(
        "#{service_name}.purge_orphan.#{node_id}", req.encode
    ) do |msg|
      @logger.debug("Purge #{node_id} Result: #{msg}")
      EM.cancel_timer(timer)
      @node_nats.unsubscribe(subscription)
      opts = SimpleResponse.decode(msg)
      if opts.success
        blk.call(success)
      else
        blk.call(wrap_error(opts))
      end
    end
  end

  def unprovision_service(instance_id, &blk)
    @logger.debug("[#{service_description}] Unprovision service #{instance_id}")
    begin
      svc = @prov_svcs[instance_id]
      raise ServiceError.new(ServiceError::NOT_FOUND, "instance_id #{instance_id}") if svc.nil?

      node_id = svc[:credentials]["node_id"]
      raise "Cannot find node_id for #{instance_id}" if node_id.nil?

      bindings = find_all_bindings(instance_id)
      @logger.debug("[#{service_description}] Unprovisioning instance #{instance_id} from #{node_id}")
      request = UnprovisionRequest.new
      request.name = instance_id
      request.bindings = bindings
      @logger.debug("[#{service_description}] Sending reqeust #{request}")
      subscription = nil
      timer = EM.add_timer(@node_timeout) {
        @node_nats.unsubscribe(subscription)
        blk.call(timeout_fail)
      }
      subscription =
        @node_nats.request(
          "#{service_name}.unprovision.#{node_id}", request.encode
       ) do |msg|
          # Delete local entries
          @prov_svcs.delete(instance_id)
          bindings.each do |b|
            @prov_svcs.delete(b[:service_id])
          end

          EM.cancel_timer(timer)
          @node_nats.unsubscribe(subscription)
          opts = SimpleResponse.decode(msg)
          if opts.success
            blk.call(success())
          else
            blk.call(wrap_error(opts))
          end
        end
    rescue => e
      if e.instance_of? ServiceError
        blk.call(failure(e))
      else
        @logger.warn(e)
        blk.call(internal_fail)
      end
    end
  end

  def provision_service(request, prov_handle=nil, &blk)
    @logger.debug("[#{service_description}] Attempting to provision instance (label=#{request['label']}, plan=#{request['plan']})")
    subscription = nil
    barrier = VCAP::Services::Base::Barrier.new(:timeout => @node_timeout, :callbacks => @nodes.length) do |responses|
      @logger.debug("[#{service_description}] Found the following nodes: #{responses.inspect}")
      @node_nats.unsubscribe(subscription)
      unless responses.empty?
        provision_node(request, responses, prov_handle, blk)
      end
    end
    subscription = @node_nats.request("#{service_name}.discover", &barrier.callback)
  rescue => e
    @logger.warn(e)
    blk.call(internal_fail)
  end

  def provision_node(request, node_msgs, prov_handle, blk)
    @logger.debug("[#{service_description}] Provisioning node (label=#{request['label']}, plan=#{request['plan']}, nodes=#{node_msgs.length})")
    nodes = node_msgs.map { |msg| Yajl::Parser.parse(msg.first) }
    best_node = nodes.max_by { |node| node_score(node) }
    if best_node && ( @allow_over_provisioning || node_score(best_node) > 0 )
      best_node = best_node["id"]
      @logger.debug("[#{service_description}] Provisioning on #{best_node}")
      prov_req = ProvisionRequest.new
      prov_req.plan = request['plan']
      # use old credentials to provision a service if provided.
      prov_req.credentials = prov_handle["credentials"] if prov_handle
      subscription = nil
      timer = EM.add_timer(@node_timeout) {
        @node_nats.unsubscribe(subscription)
        blk.call(timeout_fail)
      }
      subscription =
        @node_nats.request(
          "#{service_name}.provision.#{best_node}",
          prov_req.encode
       ) do |msg|
          EM.cancel_timer(timer)
          @node_nats.unsubscribe(subscription)
          response = ProvisionResponse.decode(msg)
          if response.success
            @logger.debug("Successfully provision response:[#{response.inspect}]")
            # credentials is not necessary in cache
            prov_req.credentials = nil
            credential = response.credentials
            svc = {:data => prov_req.dup, :service_id => credential['name'], :credentials => credential}
            # FIXME: workaround for inconsistant representation of bind handle and provision handle
            svc_local = {:configuration => prov_req.dup, :service_id => credential['name'], :credentials => credential}
            @logger.debug("Provisioned #{svc.inspect}")
            @prov_svcs[svc[:service_id]] = svc_local
            blk.call(success(svc))
          else
            blk.call(wrap_error(response))
          end
        end
    else
      # No resources
      @logger.warn("[#{service_description}] Could not find a node to provision")
      blk.call(internal_fail)
    end
  end

  def bind_instance(instance_id, binding_options, bind_handle=nil, &blk)
    @logger.debug("[#{service_description}] Attempting to bind to service #{instance_id}")

    begin
      svc = @prov_svcs[instance_id]
      raise ServiceError.new(ServiceError::NOT_FOUND, instance_id) if svc.nil?

      node_id = svc[:credentials]["node_id"]
      raise "Cannot find node_id for #{instance_id}" if node_id.nil?

      @logger.debug("[#{service_description}] bind instance #{instance_id} from #{node_id}")
      #FIXME options = {} currently, should parse it in future.
      request = BindRequest.new
      request.name = instance_id
      request.bind_opts = binding_options
      service_id = nil
      if bind_handle
        request.credentials = bind_handle["credentials"]
        service_id = bind_handle["service_id"]
      else
        service_id = UUIDTools::UUID.random_create.to_s
      end
      subscription = nil
      timer = EM.add_timer(@node_timeout) {
        @node_nats.unsubscribe(subscription)
        blk.call(timeout_fail)
      }
      subscription =
        @node_nats.request( "#{service_name}.bind.#{node_id}",
                           request.encode
       ) do |msg|
          EM.cancel_timer(timer)
          @node_nats.unsubscribe(subscription)
          opts = BindResponse.decode(msg)
          if opts.success
            opts = opts.credentials
            # Save binding-options in :data section of configuration
            config = svc[:configuration].clone
            config['data'] ||= {}
            config['data']['binding_options'] = binding_options
            res = {
              :service_id => service_id,
              :configuration => config,
              :credentials => opts
            }
            @logger.debug("[#{service_description}] Binded: #{res.inspect}")
            @prov_svcs[res[:service_id]] = res
            blk.call(success(res))
          else
            blk.call(wrap_error(opts))
          end
        end
    rescue => e
      if e.instance_of? ServiceError
        blk.call(failure(e))
      else
        @logger.warn(e)
        blk.call(internal_fail)
      end
    end
  end

  def unbind_instance(instance_id, handle_id, binding_options, &blk)
    @logger.debug("[#{service_description}] Attempting to unbind to service #{instance_id}")
    begin
      svc = @prov_svcs[instance_id]
      raise ServiceError.new(ServiceError::NOT_FOUND, "instance_id #{instance_id}") if svc.nil?

      handle = @prov_svcs[handle_id]
      raise ServiceError.new(ServiceError::NOT_FOUND, "handle_id #{handle_id}") if handle.nil?

      node_id = svc[:credentials]["node_id"]
      raise "Cannot find node_id for #{instance_id}" if node_id.nil?

      @logger.debug("[#{service_description}] Unbind instance #{handle_id} from #{node_id}")
      request = UnbindRequest.new
      request.credentials = handle[:credentials]

      subscription = nil
      timer = EM.add_timer(@node_timeout) {
        @node_nats.unsubscribe(subscription)
        blk.call(timeout_fail)
      }
      subscription =
        @node_nats.request( "#{service_name}.unbind.#{node_id}",
                           request.encode
       ) do |msg|
          EM.cancel_timer(timer)
          @node_nats.unsubscribe(subscription)
          opts = SimpleResponse.decode(msg)
          if opts.success
            @prov_svcs.delete(handle_id)
            blk.call(success())
          else
            blk.call(wrap_error(opts))
          end
        end
    rescue => e
      if e.instance_of? ServiceError
        blk.call(failure(e))
      else
        @logger.warn(e)
        blk.call(internal_fail)
      end
    end
  end

  def restore_instance(instance_id, backup_path, &blk)
    @logger.debug("[#{service_description}] Attempting to restore to service #{instance_id}")

    begin
      svc = @prov_svcs[instance_id]
      raise ServiceError.new(ServiceError::NOT_FOUND, instance_id) if svc.nil?

      node_id = svc[:credentials]["node_id"]
      raise "Cannot find node_id for #{instance_id}" if node_id.nil?

      @logger.debug("[#{service_description}] restore instance #{instance_id} from #{node_id}")
      request = RestoreRequest.new
      request.instance_id = instance_id
      request.backup_path = backup_path
      subscription = nil
      timer = EM.add_timer(@node_timeout) {
        @node_nats.unsubscribe(subscription)
        blk.call(timeout_fail)
      }
      subscription =
        @node_nats.request( "#{service_name}.restore.#{node_id}",
          request.encode
       ) do |msg|
          EM.cancel_timer(timer)
          @node_nats.unsubscribe(subscription)
          opts = SimpleResponse.decode(msg)
          if opts.success
            blk.call(success())
          else
            blk.call(wrap_error(opts))
          end
        end
    rescue => e
      if e.instance_of? ServiceError
        blk.call(failure(e))
      else
        @logger.warn(e)
        blk.call(internal_fail)
      end
    end
  end

  # Recover an instance
  # 1) Provision an instance use old credential
  # 2) restore instance use backup file
  # 3) re-bind bindings use old credential
  def recover(instance_id, backup_path, handles, &blk)
    @logger.debug("Recover instance: #{instance_id} from #{backup_path} with handles #{handles.inspect}.")
    prov_handle, binding_handles = find_instance_handles(instance_id, handles)
    @logger.debug("Provsion handle: #{prov_handle.inspect}. Binding_handles: #{binding_handles.inspect}")
    request = prov_handle["configuration"]
    provision_service(request, prov_handle) do |msg|
      if msg['success']
        updated_prov_handle = msg['response']
        # transfrom handle format
        updated_prov_handle[:configuration] = updated_prov_handle[:data]
        updated_prov_handle.delete(:data)
        updated_prov_handle = hash_sym_key_to_str(updated_prov_handle)
        @logger.info("Recover: Success re-provision instance. Updated handle:#{updated_prov_handle}")
        @update_handle_callback.call(updated_prov_handle) do |update_res|
          if not update_res
            @logger.error("Recover: Update provision handle failed.")
            blk.call(internal_fail)
          else
            @logger.info("Recover: Update provision handle success.")
            restore_instance(instance_id, backup_path) do |res|
              if res['success']
                @logger.info("Recover: Success restore instance data.")
                barrier = VCAP::Services::Base::Barrier.new(:timeout => @node_timeout, :callbacks => binding_handles.length) do |responses|
                  @logger.debug("Response from barrier: #{responses}.")
                  updated_handles = responses.select{|i| i[0] }
                  if updated_handles.length != binding_handles.length
                    @logger.error("Recover: re-bind or update handle failed. Expect #{binding_handles.length} successful responses, got #{updated_handles.length} ")
                    blk.call(internal_fail)
                  else
                    @logger.info("Recover: recover instance #{instance_id} complete!")
                    result = {
                      'success' => true,
                      'response' => "{}"
                    }
                    blk.call(result)
                  end
                end
                @logger.info("Recover: begin rebind binding handles.")
                bcb = barrier.callback
                binding_handles.each do |handle|
                  bind_instance(instance_id, nil, handle) do |bind_res|
                    if bind_res['success']
                      updated_bind_handle = bind_res['response']
                      updated_bind_handle = hash_sym_key_to_str(updated_bind_handle)
                      @logger.info("Recover: success re-bind binding: #{updated_bind_handle}")
                      @update_handle_callback.call(updated_bind_handle) do |update_res|
                        if update_res
                          @logger.error("Recover: success to update handle: #{updated_prov_handle}")
                          bcb.call(updated_bind_handle)
                        else
                          @logger.error("Recover: failed to update handle: #{updated_prov_handle}")
                          bcb.call(false)
                        end
                      end
                    else
                      @logger.error("Recover: failed to re-bind binding handle: #{handle}")
                      bcb.call(false)
                    end
                  end
                end
              else
                @logger.error("Recover: failed to restore instance: #{instance_id}.")
                blk.call(internal_fail)
              end
            end
          end
        end
      else
        @logger.error("Recover: failed to re-provision instance. Handle: #{prov_handle}.")
        blk.call(internal_fail)
      end
    end
  rescue => e
    @logger.warn(e)
    blk.call(internal_fail)
  end

  # convert symbol key to string key
  def hash_sym_key_to_str(hash)
    new_hash = {}
    hash.each do |k, v|
      if v.is_a? Hash
        v = hash_sym_key_to_str(v)
      end
      if k.is_a? Symbol
        new_hash[k.to_s] = v
      else
        new_hash[k] = v
      end
    end
    return new_hash
  end

  def on_update_service_handle(msg, reply)
    @logger.debug("[#{service_description}] Update service handle #{msg.inspect}")
    handle = Yajl::Parser.parse(msg)
    @update_handle_callback.call(handle) do |response|
      response = Yajl::Encoder.encode(response)
      @node_nats.publish(reply, response)
    end
  end

  # Gateway invoke this function to register a block which provisioner could use to update a service handle
  def register_update_handle_callback(&blk)
    @logger.debug("Register update handle callback with #{blk}")
    @update_handle_callback = blk
  end

  def varz_details()
    # Service Provisioner subclasses may want to override this method
    # to provide service specific data beyond the following

    # Mask password from varz details
    svcs = @prov_svcs.deep_dup
    svcs.each do |k,v|
      v[:credentials]['pass'] &&= MASKED_PASSWORD
      v[:credentials]['password'] &&= MASKED_PASSWORD
    end

    varz = {
      :nodes => @nodes,
      :prov_svcs => svcs
    }
    return varz
  rescue => e
    @logger.warn(e)
  end

  def healthz_details()
    healthz = {
      :self => "ok"
    }
  end

  ########
  # Helpers
  ########

  # Find instance related handles in all handles
  def find_instance_handles(instance_id, handles)
    prov_handle = nil
    binding_handles = []
    handles.each do |h|
      if h['service_id'] == instance_id
        prov_handle = h
      else
        binding_handles << h if h['credentials']['name'] == instance_id
      end
    end
    return [prov_handle, binding_handles]
  end

  # wrap a service message to hash
  def wrap_error(service_msg)
    {
      'success' => false,
      'response' => service_msg.error
    }
  end

  # Service Provisioner subclasses must implement the following
  # methods

  # node_score(node) -> number.  this base class provisions on the
  # "best" node (lowest load, most free capacity, etc). this method
  # should return a number; higher scores represent "better" nodes;
  # negative/zero scores mean that a node should be ignored
  abstract :node_score

  # service_name() --> string
  # (inhereted from VCAP::Services::Base::Base)

end
