# Copyright (c) 2009-2011 VMware, Inc.
require "base/provisioner"
require "sqlfire_service/common"
require "uuidtools"

class VCAP::Services::Sqlfire::Provisioner < VCAP::Services::Base::Provisioner

  VALID_CREDENTIAL_CHARACTERS = ("A".."Z").to_a + ("a".."z").to_a + ("0".."9").to_a

  include VCAP::Services::Sqlfire::Common

  def node_score(node)
    node['available_memory']
  end

  # Custom provisioner which handles scaling of the service according to 'plan'.
  #
  # When provisioning this service, the user can select the 'size' of their
  # distributed database by choosing a particular plan. Each plan translates to
  # a fixed number of nodes on which a sqlfire vm will be started.
  #
  # The high level steps are as follows:
  #
  #   Figure out how many nodes we need to provision.
  #   Look at the nodes we have (node_msgs) and select the 'best' nodes.
  #   On the first node found, first start a locator and a server.
  #   On all remaining nodes, start a server passing in the address of the
  #   newly-created locator.
  #
  # If any one component fails to start the whole service will fail.
  # If not enough nodes exist to fulfill the plan then only the available nodes
  # are used, however each node will only have a single vm provisioned (except
  # for the locator).
  #
  def provision_node(request, node_msgs, prov_handle, blk)
    @logger.debug("[#{service_description}] Provisioning nodes (nnodes=#{node_msgs.length})")

    plan = request.plan || "free"
    # Translate the chosen plan into a number of nodes
    want_nodes = @service[:plan_mapping][plan.to_sym] || 1

    nodes = node_msgs.map { |msg| Yajl::Parser.parse(msg.first) }
    sorted_nodes = nodes.sort_by { |node| -(node_score(node)) }

    # We may get back less than we actually have - that's OK
    best_nodes = sorted_nodes.take(want_nodes)
    @logger.debug("[#{service_description}] Plan '#{plan}' indicates #{want_nodes} node(s) - we have #{best_nodes.length} node(s)")

    # Other services let the node handle this, however we need these values to
    # be consistent across all vms that make up the particular instance of this
    # service. These values are then passed to each node as part of the
    # provision request.
    svc_name = "sqlfire-#{UUIDTools::UUID.random_create.to_s}"
    user_name = "U_" + generate_credential
    user_password = "P_" + generate_credential

    prov_req = ProvisionRequest.new
    prov_req.plan = request.plan
    prov_req.name = svc_name
    prov_req.options = {:locator => "", :user => user_name, :password => user_password}

    # use old credentials to provision a service if provided.
    prov_req.credentials = prov_handle["credentials"] if prov_handle

    node = best_nodes.shift
    node_id = node["id"]
    @logger.debug("[#{service_description}] Provisioning sqlfire on #{node_id}")
    subscription = nil
    timer = EM.add_timer(@node_timeout) {
      @node_nats.unsubscribe(subscription)
      blk.call(timeout_fail)
    }

    # Uggh. The asynchronous nature of message passing makes this code rather
    # convoluted. The first request is sent to the node which will start the
    # locator and one server process. We need to wait for this to complete (so we know
    # the locator is running) before attempting to provision all the other
    # nodes.
    subscription =
      @node_nats.request(
        "#{service_name}.provision.#{node_id}",
        prov_req.encode
      ) do |msg|
        EM.cancel_timer(timer)
        @node_nats.unsubscribe(subscription)
        response = ProvisionResponse.decode(msg)
        if response.success
          @logger.debug("Successful provision response:[#{response.inspect}]")
          initial_req = prov_req.deep_dup

          # credentials is not necessary in cache
          initial_req.credentials = nil
          credential = response.credentials
          svc = {:data => initial_req.dup, :service_id => credential['name'], :credentials => credential}
          # FIXME: workaround for inconsistant representation of bind handle and provision handle
          svc_local = {:configuration => initial_req.dup, :service_id => credential['name'], :credentials => credential}
          @prov_svcs[svc[:service_id]] = svc_local

          node_count = best_nodes.size
          if node_count > 0
            # For the remaining nodes we need to pass in the address of the locator.
            prov_req.options[:locator] = "#{response.credentials['hostname']}[#{response.credentials['locator_port']}]"

            # Now let's provision all the other nodes we want
            best_nodes.each do |node|
              node_id = node["id"]
              @logger.debug("[#{service_description}] Provisioning sqlfire on #{node_id}")
              @node_nats.request(
                "#{service_name}.provision.#{node_id}",
                prov_req.encode
              ) do |msg|
                # node_count will be decremented for every response we receive.
                node_count -= 1
                response = ProvisionResponse.decode(msg)
                if response.success
                  @logger.debug("Successful provision response:[#{response.inspect}]")
                else
                  blk.call(wrap_error(response))
                end
                @logger.debug("Provisioned #{svc.pretty_inspect}")

                # Once we've received all the responses we're done.
                blk.call(success(svc)) if node_count == 0
              end
            end
          else
            @logger.debug("Provisioned #{svc.pretty_inspect}")
            blk.call(success(svc))
          end
        else
          blk.call(wrap_error(response))
        end
      end
  end


  def unprovision_service(instance_id, &blk)
    @logger.debug("[#{service_description}] Attempting to unprovision service #{instance_id}")
    subscription = nil
    barrier = VCAP::Services::Base::Barrier.new(:timeout => @node_timeout, :callbacks => @nodes.length) do |responses|
      @logger.debug("[#{service_description}] Found the following nodes: #{responses.pretty_inspect}")
      @node_nats.unsubscribe(subscription)
      nodes = responses.map { |msg| Yajl::Parser.parse(msg.first) }
      filtered_nodes = nodes.find_all { |r| r["services"].include?(instance_id) }
      unless filtered_nodes.empty?
        unprovision_node(instance_id, filtered_nodes, blk)
      end
    end
    subscription = @node_nats.request("#{service_name}.discover", &barrier.callback)
  rescue => e
    @logger.warn(e)
    blk.call(internal_fail)
  end


  def unprovision_node(instance_id, nodes, blk)
    @logger.debug("[#{service_description}] Unprovisioning service #{instance_id} on #{nodes.size} node(s)")
    begin
      svc = @prov_svcs[instance_id]
      raise ServiceError.new(ServiceError::NOT_FOUND, "instance_id #{instance_id}") if svc.nil?

      node_id = svc[:credentials]["node_id"]
      raise "Cannot find node_id for #{instance_id}" if node_id.nil?

      bindings = find_all_bindings(instance_id)
      request = UnprovisionRequest.new
      request.name = instance_id
      request.bindings = bindings
#      @logger.debug("[#{service_description}] Sending reqeust #{request}")

      subscriptions = []
      timer = EM.add_timer(@node_timeout) {
        subscriptions.each { |s| @node_nats.unsubscribe(s) }
        blk.call(timeout_fail)
      }

      node_count = nodes.size
      errors = 0
      last_error = nil
      nodes.each do |node|
        node_id = node["id"]
        @logger.debug("[#{service_description}] Unprovisioning instance #{instance_id} on #{node_id}")
        subscription = @node_nats.request("#{service_name}.unprovision.#{node_id}", request.encode) do |msg|
          @node_nats.unsubscribe(subscription)
          opts = SimpleResponse.decode(msg)
          if !opts.success
            last_error = opts
            errors += 1
          end
          node_count -= 1

          if node_count == 0
            # Delete local entries
            @prov_svcs.delete(instance_id)
            bindings.each do |b|
              @prov_svcs.delete(b[:service_id])
            end

            EM.cancel_timer(timer)
            if errors == 0
              blk.call(success())
            else
              blk.call(wrap_error(last_error))
            end
          end
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


  def generate_credential(length=12)
    Array.new(length) { VALID_CREDENTIAL_CHARACTERS[rand(VALID_CREDENTIAL_CHARACTERS.length)] }.join
  end
end
