# Copyright (c) 2009-2012 VMware, Inc.
require 'fiber'
require 'dm-types'
require 'nats/client'
require 'uuidtools'

module VCAP
  module Services
    module AppDirect
      class AsynchronousServiceGateway < VCAP::Services::AsynchronousServiceGateway

        class AppDirectService
          include DataMapper::Resource
          # Custom table name
          storage_names[:default] = "appdirect_services"

          property :label,       String,   :key => true
          property :name,        String,   :required => true
          property :version,     String,   :required => true
          property :provider,    String
          property :plan,        String
          property :credentials, Json
          property :acls,        Json
        end

        VMWARE_ACLS   = ["*@vmware.com", "*@rubicon.com"]
        REQ_OPTS      = %w(mbus external_uri token cloud_controller_uri).map {|o| o.to_sym}
        API_VERSION   = "poc"

        set :raise_errors, Proc.new {false}
        set :show_exceptions, false

        attr_accessor :last_response

        def initialize(opts)
          super(opts)
        end

        def setup(opts)
          missing_opts = REQ_OPTS.select {|o| !opts.has_key? o}
          raise ArgumentError, "Missing options: #{missing_opts.join(', ')}" unless missing_opts.empty?

          @host                  = opts[:host]
          @port                  = opts[:port]
          @node_timeout          = opts[:node_timeout]
          @logger                = opts[:logger] || make_logger()
          @token                 = opts[:token]
          @hb_interval           = opts[:heartbeat_interval] || 60
          @cld_ctrl_uri          = http_uri(opts[:cloud_controller_uri])
          @external_uri          = opts[:external_uri]
          @offering_uri          = "#{@cld_ctrl_uri}/services/v1/offerings/"
          @service_list_uri      = "#{@cld_ctrl_uri}/appdirect_services/poc/offerings"
          @router_start_channel  = nil
          @proxy_opts            = opts[:proxy]
          @ready_to_serve        = false
          @handle_fetched        = true # set to true in order to compatible with base asycn gateway.
          @router_register_json  = {
            :host => @host,
            :port => @port,
            :uris => [ @external_uri ],
            :tags => {:components =>  "AppDirect"},
          }.to_json

          @helper = VCAP::Services::AppDirect::Helper.new(opts, @logger)
          @catalog = nil

          token_hdrs = VCAP::Services::Api::GATEWAY_TOKEN_HEADER
          @cc_req_hdrs           = {
            'Content-Type' => 'application/json',
            token_hdrs     => @token,
          }

          driver, path = opts[:local_db].split(':')
          db_dir = File.dirname(path)
          FileUtils.mkdir_p(db_dir)

          DataMapper.setup(:default, opts[:local_db])
          DataMapper::auto_upgrade!

          #Kernel.at_exit do
          #  if EM.reactor_running?
          #    on_exit(false)
          #  else
          #    EM.run { on_exit }
          #  end
          #end

          ##### Start up
          f = Fiber.new do
            begin
              # get all AppDirect service offerings
              fetch_appdirect_services
              # active services in local database
              advertise_saved_services
              # Ready to serve
              @logger.info("AppDirect Gateway is ready to serve incoming request.")
              @ready_to_serve = true
            rescue => e
              @logger.fatal("Error when start up: #{fmt_error(e)}")
            end
          end
          f.resume
        end

        # Validate the incoming request
        before do
          unless @ready_to_serve
            error_msg = ServiceError.new(ServiceError::SERVICE_UNAVAILABLE).to_hash
            abort_request(error_msg)
          end
        end

        error [JsonMessage::ValidationError, JsonMessage::ParseError] do
          error_msg = ServiceError.new(ServiceError::MALFORMATTED_REQ).to_hash
          abort_request(error_msg)
        end

        not_found do
          error_msg = ServiceError.new(ServiceError::NOT_FOUND, request.path_info).to_hash
          abort_request(error_msg)
        end

        def on_connect_nats()
          @logger.info("Register service broker uri : #{@router_register_json}")
          @nats.publish('router.register', @router_register_json)
          @router_start_channel = @nats.subscribe('router.start') { @nats.publish('router.register', @router_register_json)}
        end

        def fetch_appdirect_services
          @catalog = @helper.get_catalog()
        end

        def advertise_saved_services(active=true)
          @catalog.each do |name, bsvc|
            req = {}
            req[:label] = name
            req[:active] = bsvc["active"]
            req[:description] = bsvc["description"]

            if bsvc["developers"] and bsvc["developers"].count > 0
              acls = []
              bsvc["developers"].each do |dev|
                acls << dev["email"]
              end
              req[:acls] = {}
              req[:acls][:wildcards] = VMWARE_ACLS
              req[:acls][:users] = acls
            end

            req[:url] = "http://#{@external_uri}"

            if bsvc["plans"] and bsvc["plans"].count > 0
              req[:plans] = []
              bsvc["plans"].each do |plan|
                req[:plans] << plan["id"]
                # No plan options yet
              end
            else
              req[:plans] = ["default"]
            end

            # No tags coming from AppDirect yet
            req[:tags] = ["default"]
            advertise_appdirect_service_to_cc(req)
          end
        end

        def stop_nats()
          @nats.unsubscribe(@router_start_channel) if @router_start_channel
          @logger.debug("Unregister uri: #{@router_register_json}")
          @nats.publish("router.unregister", @router_register_json)
          @nats.close
        end

        def on_exit(stop_event_loop=true)
          @ready_to_serve = false
          Fiber.new {
            advertise_saved_services(false)
            stop_nats
            EM.stop if stop_event_loop
          }.resume
        end

        #################### Handlers ###################

        # Provision a brokered service
        post "/gateway/v1/configurations" do
          req =  VCAP::Services::Api::GatewayProvisionRequest.decode(request_body)
          @logger.info("Provision request for label=#{req.label} plan=#{req.plan}")

          Fiber.new {
            msg = provision_appdirect_service(req)
            if msg['success']
              async_reply(VCAP::Services::Api::GatewayProvisionResponse.new(msg['response']).encode)
            else
              async_reply_error(msg['response'])
            end
          }.resume
          async_mode
        end

        # Binding a brokered service
        post "/gateway/v1/configurations/:service_id/handles" do
          req = VCAP::Services::Api::GatewayBindRequest.decode(request_body)
          @logger.info("Binding request for service=#{params['service_id']} options=#{req.binding_options}")

          Fiber.new {
            msg = bind_appdirect_service_instance(req.label, req.service_id, req.binding_options)
            if msg['success']
              async_reply(VCAP::Services::Api::GatewayBindResponse.new(msg['response']).encode)
            else
              async_reply_error(msg['response'])
            end
          }.resume
          async_mode
        end

        # Unprovisions a brokered service instance
        delete "/gateway/v1/configurations/:service_id" do
          @logger.debug("Unprovision request for service_id=#{params['service_id']}")
          Fiber.new {
            #TODO
          }.resume
          async_mode
        end

        # Unbinds a brokered service instance
        delete "/gateway/v1/configurations/:service_id/handles/:handle_id" do
          @logger.info("Unbind request for service_id=#{params['service_id']} handle_id=#{params['handle_id']}")
          Fiber.new {
            #TODO
          }.resume
          async_mode
        end

        ################## Helpers ###################
        #
        helpers do

          def advertise_appdirect_service_to_cc(offering)
            @logger.debug("advertise service offering to cloud_controller:#{offering.inspect}")
            return false unless offering

            req = create_http_request(
              :head => @cc_req_hdrs,
              :body => Yajl::Encoder.encode(offering),
            )

            f = Fiber.current
            http = EM::HttpRequest.new(@offering_uri).post(req)
            http.callback do
              @last_response = http.response
              f.resume(http)
            end
            http.errback { f.resume(http) }
            Fiber.yield

            if http.error.empty?
              if http.response_header.status == 200
                @logger.info("Successfully advertise offerings #{offering.inspect}")
                return true
              else
                @logger.warn("Failed advertise offerings:#{offering.inspect}, status=#{http.response_header.status}")
              end
            else
              @logger.warn("Failed advertise offerings:#{offering.inspect}: #{http.error}")
            end
            return false
          end

          def delete_offerings(label)
            return false unless label

            req = create_http_request(:head => @cc_req_hdrs)
            uri = URI.join(@offering_uri, label)
            f = Fiber.current
            http = EM::HttpRequest.new(uri).delete(req)
            http.callback { f.resume(http) }
            http.errback { f.resume(http) }
            Fiber.yield

            if http.error.empty?
              if http.response_header.status == 200
                @logger.info("Successfully delete offerings label=#{label}")
                return true
              else
                @logger.warn("Failed delete offerings label=#{label}, status=#{http.response_header.status}")
              end
            else
              @logger.warn("Failed delete offerings label=#{label}: #{http.error}")
            end
            return false
          end

          def provision_appdirect_service(request)
            appdirect_svc = AppDirectService.get(request.label)
            if appdirect_svc
              order = {
                "email" => request.email,
                "productName" => request.name,
                "plan" => request.plan,
                "productId" => request.label
              }

              # TODO: Consider doing a lookup in appdirect to get a user id by email
              user_id = request.email
              receipt = @helper.purchase_service(order, user_id)

              if receipt
                credentials = receipt["credentials"]
                @logger.debug("AppDirect service provisioned #{svc.inspect}")
                svc = {
                  :data => {:plan => receipt["data"]["plan"]},
                  :credentials => credentials,
                  :service_id => receipt["order"],
                }
                success(svc)
              else
                @logger.warn("Invalid request to provision service label=#{request.label}")
                raise ServiceError.new(ServiceError::NOT_FOUND, request.label)
              end
            else
              @logger.warn("Can't find service label=#{request.label}")
              raise ServiceError.new(ServiceError::NOT_FOUND, request.label)
            end
          rescue => e
            if e.instance_of? ServiceError
              failure(e)
            else
              @logger.warn("Can't provision service label=#{request.label}: #{fmt_error(e)}")
              internal_fail
            end
          end

          def bind_appdirect_service_instance(label, instance_id, binding_options, bind_handle=nil)
            bsvc = AppDirectService.get(label)
            if bsvc
              binding = {
                :configuration => {:data => {:binding_options => binding_options}},
                :credentials => bsvc.credentials,
                :service_id => UUIDTools::UUID.random_create.to_s,
              }
              @logger.debug("Generate new service binding: #{binding.inspect}")
              success(binding)
            else
              @logger.warn("Can't find service label=#{label}")
              raise ServiceError.new(ServiceError::NOT_FOUND, label)
            end
          rescue => e
            if e.instance_of? ServiceError
              failure(e)
            else
              @logger.warn("Can't bind service label=#{label}, id=#{instance_id}: #{fmt_error(e)}")
              internal_fail
            end
          end

          def fmt_error(e)
            "#{e} [#{e.backtrace.join("|")}]"
          end
        end

      end
    end
  end
end

