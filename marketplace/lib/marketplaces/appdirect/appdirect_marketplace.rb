# Copyright (c) 2009-2012 VMware, Inc.
require 'fiber'
require 'service_error'
require_relative 'appdirect_helper'

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..')
require 'base/marketplace_base'

module VCAP
  module Services
    module Marketplace
      module Appdirect
        class AppdirectMarketplace < VCAP::Services::Marketplace::Base

          include VCAP::Services::Base::Error
          include VCAP::Services::Marketplace::Appdirect

          def initialize(opts)
            super(opts)

            @logger       = opts[:logger]
            @external_uri = opts[:external_uri]
            @node_timeout = opts[:node_timeout]
            @acls         = opts[:acls]
            @helper       = AppdirectHelper.new(opts, @logger)
            @mapping      = opts[:offering_mapping] || {}

            @cc_api_version = opts[:cc_api_version]

            # Maintain a reverse mapping since we'll be changing the service name for CC advertisement
            # A provision request will require the actual service name rather than the one in CCDB
            @service_id_map = {}
            @mapping.keys.each { |k|
              service_name = @mapping[k.to_sym][:name]
              @service_id_map[service_name] = k.to_s
            }
          end

          def name
            "AppDirect"
          end

          def get_catalog
            appdirect_catalog = @helper.load_catalog
            catalog = {}
            appdirect_catalog.each { |s|
              mapping = load_mapped_name_and_provider(s["label"], s["provider"])
              name     = mapping[:name]
              provider = mapping[:provider]

              version = s["version"] || "1.0" # UNTIL AD fixes this...
              key = key_for_service(name, version, provider)

              # Setup acls
              # TODO: Use per service offering acls
              acls = @acls

              # Setup plans
              plans = {}
              if s["plans"] and s["plans"].count > 0
                s["plans"].each do |plan|
                  plans[plan["id"]] = { "description" => plan["description"], "free" => plan["free"] }
                end
              end

              # Finally, generate the catalog entry
              catalog[key] = {
                "id"          => name,
                "version"     => version,
                "description" => s["description"],
                "info_url"    => s["info_url"],
                "plans"       => plans,
                "provider"    => provider,
                "acls"        => acls,
                "url"         => @external_uri,
                "timeout"     => @node_timeout,
                "tags"        => [], # unused in ccng, in cc a non-null value to allow tags clone during bind
              }
            }
            catalog
          end

          def load_mapped_name_and_provider(name, provider)
            @mapping[name.to_sym] if @mapping.keys.include?(name.to_sym)

            # If mapping is not defined, then just use defaults
            { :name => name, :provider => provider }
          end

          def offering_disabled?(id, offerings_list)
            # Translate service name if a custom mapping was defined
            id = @service_id_map[id] if @service_id_map.keys.include?(id)

            # Check if its still listed
            @logger.info("Offering: #{id} - Present in offering list: #{offerings_list.include?(id)}")
            !(offerings_list.include?(id))
          end

          ##### Handle the 4 operations #####

          def provision_service(request_body)
            request =  VCAP::Services::Api::GatewayProvisionRequest.decode(request_body)
            id,version = request.label.split("-")
            id = @service_id_map[id] if @service_id_map.keys.include?(id)

            # TODO: Temporary measure until we fix gateway provision request to send us provider
            mapping  = load_mapped_name_and_provider(id, nil)
            provider = mapping[:provider] # TODO: Replace with request.provider

            @logger.debug("Provision request for offering: #{request.label} (id=#{id}) provider=#{provider}, plan=#{request.plan}, version=#{request.version}")

            order = {
              "user" => {
                "uuid"  => request.user_guid,
                "email" => request.email
              },
              "offering" => {
                "label"    => id,
                "provider" => provider
              },
              "configuration" => {
                "plan" => request.plan,
                "name" => request.name,
              },
              "billing" => {
                "space_guid" => request.space_guid
              }
            }
            receipt = @helper.purchase_service(order)

            @logger.debug("AppDirect service provisioned #{receipt.inspect}")
            credentials = receipt["credentials"] || {}
            credentials["name"] = receipt["id"] #id of service within the 3rd party ISV
            #We could store more info in credentials but these will never be used by apps or users
            {
              :configuration => {:plan => request.plan, :name => request.name, :options => {} },
              :credentials => credentials,
              :service_id => receipt["uuid"],
            }
          end

          def unprovision_service(service_id)
            @helper.cancel_service(service_id)
          end

          def bind_service_instance(service_id, binding_options)
            order = { "options" => binding_options }

            resp = @helper.bind_service(order, service_id)
            @logger.debug("Bind response from AppDirect: #{resp.inspect}")
            {
              :configuration => {:data => {:binding_options => binding_options}},
              :credentials => resp["credentials"],
              :service_id => resp["uuid"],  #Important this is the binding_id
            }
          end

          def unbind_service(service_id, binding_id)
            @helper.unbind_service(service_id, binding_id)
          end

        end
      end
    end
  end
end
