# Copyright (c) 2009-2012 VMware, Inc.
require "oauth"
#require "json_message"
require "json"
require_relative "appdirect_error"

#class VCAP::Services::AppDirect::AppDirectCatalogResponse < JsonMessage
#
#end

module VCAP
  module Services
    module AppDirect
      class Helper

        include VCAP::Services::AppDirect

        OFFERINGS_PATH = "custom/cloudfoundry/v1/offerings"
        SERVICES_PATH = "custom/cloudfoundry/v1/services"

        def initialize(appdirect_config, logger)
          @logger = logger

          @logger.debug("Got config #{appdirect_config.to_yaml}")

          raise("No appdirect provided provided") unless appdirect_config[:appdirect]

          @scheme =  appdirect_config[:appdirect][:scheme] || raise("No scheme provided")
          @host = appdirect_config[:appdirect][:host] || raise("No host provided")
          @appdirect_key = appdirect_config[:appdirect][:key] || raise("No Key Provided")
          @appdirect_secret = appdirect_config[:appdirect][:secret] || raise("No secret provided") unless @appdirect_secret

          @consumer = OAuth::Consumer.new(@appdirect_key,  @appdirect_secret)
          @access_token = OAuth::AccessToken.new(@consumer)
        end

        def get_catalog
          catalog = nil
          url = "#{@scheme}://#{@host}/api/#{OFFERINGS_PATH}"
          @logger.info("About to get service listing from #{url}")
          response = @access_token.get(url, { 'Accept'=>'application/json' })
          raw = response.body
          if response.code == "200"
            data = JSON.parse(raw) #VCAP::Services::AppDirect::AppDirectCatalogResponse.decode(raw)
            catalog = {}
            data.each do |service|
              # Add checks for specific categories which determine whether the addon should be listed on cc
              @logger.info("Processing #{service["id"]}")
              catalog[service["id"]] = service
            end
            @logger.info("Got #{catalog.keys.count} items from AppDirect")
          else
            @logger.error("Got error body #{response.body}")
            raise AppDirectError.new(AppDirectError::APPDIRECT_ERROR_GET_LISTING, response.code)
          end
          catalog
        end

        def purchase_service(order)
          new_serv = nil
          # TODO: Order needs to include UUID for User, Organization, AppSpace
          if order
            url = "#{@scheme}://#{@host}/api/#{SERVICES_PATH}"
            body = order.to_json
            response = @access_token.post(url, body, { 'Accept'=>'application/json' })
            if response.code == "201" or response.code == "200"
              new_serv = JSON.parse(response.body)
              return new_serv
            else
              # 400 bad request
              # 500 if AppDirect has issues
              # 503 if ISV is down
              raise AppDirectError.new(AppDirectError::APPDIRECT_ERROR_PURCHASE, response.code)
            end
          else
            @logger.error("Order is required to purchase a service")
          end
          new_serv
        end

        def bind_service(order, order_id)
          update_serv = nil
          if order and order_id
            url = "#{@scheme}://#{@host}/api/#{SERVICES_PATH}/#{order_id}/bindings"
            body = order.to_json
            response = @access_token.post(url, body, { 'Accept'=>'application/json' })
            if response.code == "200"
              update_serv = JSON.parse(response.body)
              @logger.debug("Bound service #{order_id}")
            else
              raise AppDirectError.new(AppDirectError::APPDIRECT_ERROR_BIND, response.code)
            end
          else
            @logger.error("Order and Order Id are required to cancel a service")
          end
          update_serv
        end

        def unbind_service(binding_id, order_id)
          update_binding = false
          if binding_id and order_id
            url = "#{@scheme}://#{@host}/api/#{SERVICES_PATH}/#{order_id}/bindings/#{binding_id}"
            response = @access_token.delete(url, { 'Accept'=>'application/json' })
            if response.code == "200"
              update_binding = true
            else
              raise AppDirectError.new(AppDirectError::APPDIRECT_ERROR_UNBIND, response.code)
            end
          else
            @logger.error("Binding Id, Order Id and User Id are required to cancel a service")
          end
          update_binding
        end

        def cancel_service(order_id)
          cancel_serv = false
          if order_id
            url = "#{@scheme}://#{@host}/api/#{SERVICES_PATH}/#{order_id}"
            response = @access_token.delete(url, { 'Accept'=>'application/json' })
            if response.code == "204" or response.code == "200"
              @logger.debug("Deleted #{order_id}")
              cancel_serv = true
            else
              raise AppDirectError.new(AppDirectError::APPDIRECT_ERROR_CANCEL, response.code)
            end
          else
            @logger.error("Order Id is required to cancel a service")
          end
          cancel_serv
        end
      end
    end
  end
end
