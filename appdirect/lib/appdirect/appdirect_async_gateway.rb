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

          Kernel.at_exit do
            if EM.reactor_running?
              on_exit(false)
            else
              EM.run { on_exit }
            end
          end

          ##### Start up
          f = Fiber.new do
            begin
              # get all AppDirect service offerings
              fetch_appdirect_services
              # active services in local database
              advertise_services
              # Ready to serve
              @ready_to_serve = true
              @logger.info("AppDirect Gateway is ready to serve incoming request.")
            rescue => e
              @logger.fatal("Error when start up: #{fmt_error(e)}")
            end
          end
          f.resume
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
          @catalog = @helper.get_catalog
        end

        def advertise_services(active=true)
          if @catalog
            @catalog.each do |name, bsvc|
              req = {}
              req[:label] = "#{name}-#{bsvc["version"]}"
              req[:active] = active && bsvc["active"]
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
        end

        def stop_nats()
          @nats.unsubscribe(@router_start_channel) if @router_start_channel and @nats
          @logger.debug("Unregister uri: #{@router_register_json}")
          if @nats
            @nats.publish("router.unregister", @router_register_json)
            @nats.close
          end
        end

        def on_exit(stop_event_loop=true)
          @ready_to_serve = false
          Fiber.new {
            # Since the services are not being stored locally
            fetch_appdirect_services
            advertise_services(false)
            stop_nats
            EM.stop if stop_event_loop
          }.resume
        end

        #################### Handlers ###################

        get "/" do
          return {"offerings" => @catalog, "ready_to_serve" => @ready_to_serve}.to_json
        end

        # Provision a brokered service
        post "/gateway/v1/configurations" do
          @logger.info("Got request_body=#{request_body}")

          Fiber.new{
            msg = provision_appdirect_service(request_body)
            if msg['success']
              resp = VCAP::Services::Api::GatewayProvisionResponse.new(msg['response'])
              resp = resp.encode
              async_reply(resp)
            else
              async_reply_error(msg['response'])
            end
          }.resume
          async_mode
        end

        # Binding a brokered service
        post "/gateway/v1/configurations/:service_id/handles" do
          @logger.info("Got request_body=#{request_body}")
          #req = JSON.parse(request_body)
          req = VCAP::Services::Api::GatewayBindRequest.decode(request_body)
          @logger.info("Binding request for service=#{params['service_id']} options=#{req.inspect}")

          Fiber.new {
            # {"label":"mongolab-2.0","name":"m1","email":"mwilkinson@vmware.com","plan":"free","plan_option":null,"version":"2.0"}
            msg = bind_appdirect_service_instance(params['service_id'], req)
            if msg['success']
              resp = VCAP::Services::Api::GatewayBindResponse.new(msg['response'])
              resp = resp.encode
              async_reply(resp)
            else
              async_reply_error(msg['response'])
            end
          }.resume
          async_mode
        end

        # Unprovisions a brokered service instance
        delete "/gateway/v1/configurations/:service_id" do
          sid = params['service_id']
          @logger.debug("Unprovision request for service_id=#{sid}")
          Fiber.new {
            if unprovision_service(sid)
              async_reply
            else
              async_reply_error("Could not unprovision service #{sid}")
            end

          }.resume
          async_mode
        end

        # Unbinds a brokered service instance
        delete "/gateway/v1/configurations/:service_id/handles/:handle_id" do
          sid = params['service_id']
          hid = params['handle_id']
          @logger.info("Unbind request for service_id=#{sid} handle_id=#{hid}")
          Fiber.new {
            if unbind_service(sid, hid)
              async_reply
            else
              async_reply_error("Could not unbind service #{sid} with handle id #{hid}")
            end

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

          def provision_appdirect_service(request_body)
            request =  VCAP::Services::Api::GatewayProvisionRequest.decode(request_body)
            @logger.info("Provision request for label=#{request.label} plan=#{request.plan}")

            id,version = request.label.split("-")

            order = {
                "user" => {
                    "uuid" => nil,
                    "email" => request.email
                },
                "offering" => {
                    "id" => id,
                    "version" => request.version || version
                },
                "configuration" => {
                    "plan" => request.plan,
                    "name" => request.name,
                    "options" => {
                    }
                }
            }
            receipt = @helper.purchase_service(order)

            if receipt
              @logger.debug("AppDirect service provisioned #{receipt.inspect}")
              credentials = receipt["credentials"] || {}
              credentials["name"] = receipt["id"] #id of service within the 3rd party ISV
              #We could store more info in credentials but these will never be used by apps or users
              svc = {
                :data => {:plan => receipt["configuration"]["plan"]},
                :credentials => credentials,
                :service_id => receipt["uuid"],
              }
              success(svc)
            else
              @logger.warn("Invalid response to provision service label=#{request.label}")
              raise ServiceError.new(ServiceError::INTERNAL_ERROR, "Missing request -- cannot perform operation")
            end
          rescue => e
            if e.instance_of? ServiceError
              failure(e)
            else
              @logger.debug(e.inspect)
              @logger.warn("Can't provision service label=#{request.label}: #{fmt_error(e)}")
              internal_fail
            end
          end

          def unprovision_service(service_id)
            success = @helper.cancel_service(service_id)
            if success
             @logger.info("Successfully unprovisioned service #{service_id}")
            else
              @logger.info("Failed to unprovision service #{service_id}")
            end
            return success
          rescue => e
            if e.instance_of? ServiceError
              failure(e)
            else
              @logger.warn("Can't unprovision service service_id=#{service_id}: #{fmt_error(e)}")
              internal_fail
            end
          end

          def bind_appdirect_service_instance(service_id, request)
            if service_id and request
              order = {
                "options" => request.binding_options
              }
              resp = @helper.bind_service(order, service_id)
              @logger.debug("Got response from AppDirect: #{resp.inspect}")
              binding = {
                :configuration => {:data => {:binding_options => request.binding_options}},
                :credentials => resp["credentials"],
                :service_id => resp["uuid"],  #Important this is the binding_id
              }
              @logger.debug("Generated binding for CC: #{binding.inspect}")
              success(binding)
            else
              @logger.warn("Can't find service label=#{label}")
              raise ServiceError.new(ServiceError::INTERNAL_ERROR, "Missing request or service_id -- cannot perform operation")
            end
          rescue => e
            if e.instance_of? ServiceError
              failure(e)
            else
              @logger.warn("Can't bind service service_id=#{service_id}, request=#{request}: #{fmt_error(e)}")
              internal_fail
            end
          end

          def unbind_service(service_id, binding_id)
            success = @helper.unbind_service(service_id, binding_id)
              if success
               @logger.info("Successfully unbound service #{service_id} and binding id #{binding_id}")
              else
                @logger.info("Failed to unbind service #{service_id} and binding id #{binding_id}")
              end
              return success
            rescue => e
              if e.instance_of? ServiceError
                failure(e)
              else
                @logger.warn("Can't unprovision service service_id=#{service_id}: #{fmt_error(e)}")
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

