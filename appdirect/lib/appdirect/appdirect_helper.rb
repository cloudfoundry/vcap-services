# Copyright (c) 2009-2012 VMware, Inc.
require "oauth"
#require "json_message"
require "json"
#require_relative "appdirect_error"

#class VCAP::Services::AppDirect::AppDirectCatalogResponse < JsonMessage
#
#end

module VCAP
  module Services
    module AppDirect
      class Helper

        include VCAP::Services::AppDirect

        def initialize(appdirect_config, logger)
          @logger = logger

          @logger.debug("Got config #{appdirect_config.to_yaml}")

          @host = appdirect_config[:appdirect][:host] || raise("No host provided")
          @appdirect_key = appdirect_config[:appdirect][:key] || raise("No Key Provided")
          @appdirect_secret = appdirect_config[:appdirect][:secret] || raise("No secret provided") unless @appdirect_secret

          @consumer = OAuth::Consumer.new(@appdirect_key,  @appdirect_secret)
          @access_token = OAuth::AccessToken.new(@consumer)
        end

        def get_catalog
          catalog = nil
          url = "https://#{@host}/api/marketplace/v1/listing"
          response = @access_token.get(url, { 'Accept'=>'application/json' })
          raw = response.body
          if response.code == "200"
            data = JSON.parse(raw) #VCAP::Services::AppDirect::AppDirectCatalogResponse.decode(raw)
            catalog = {}
            data.each do |service|
              # Add checks for specific categories which determine whether the addon should be listed on cc
              if service["free"] and service["buyable"]
                @logger.debug("Processing #{service["name"]}")
                catalog[service["name"]] = service
              end
            end
            @logger.debug("Got #{catalog.keys.count} items from AppDirect")
          else
            raise AppDirectError.new(AppDirectError::APPDIRECT_ERROR_GET_LISTING, response.code)
          end
          catalog
        end

        def purchase_service(order, user_id)
          new_serv = nil
          if order and user_id
            url = "https://#{@host}/api/account/v1/users/#{user_id}/orders"
            body = order.to_json
            response = @access_token.post(url, body, { 'Accept'=>'application/json' })
            if response.code == "200"
              new_serv = JSON.parse(response.body)
              if new_serv and new_serv.keys.include?("productId") and new_serv.keys.include?("order")
                return new_serv
              else
                new_serv = nil
                @logger.error("Unexpected body from AppDirect #{response.body}")
              end
            else
              raise AppDirectError.new(AppDirectError::APPDIRECT_ERROR_PURCHASE, response.code)
            end
          else
            @logger.error("Order and Product Id are required to purchase a service")
          end
          new_serv
        end

        def bind_service(order, user_id, order_id)
          update_serv = nil
          if order and user_id and order_id
            url = "https://#{@host}/api/account/v1/users/#{user_id}/orders/#{order_id}/apps"
            body = order.to_json
            response = @access_token.post(url, body, { 'Accept'=>'application/json' })
            if response.code == "200"
              update_serv = JSON.parse(response.body)
              @logger.debug("Upated #{order_id} for #{user_id}")
            else
              raise AppDirectError.new(AppDirectError::APPDIRECT_ERROR_BIND, response.code)
            end
          else
            @logger.error("Order, Order Id and Product Id are required to cancel a service")
          end
          update_serv
        end

        def unbind_service(binding_id, user_id, order_id)
          update_binding = nil
          if binding_id and user_id and order_id
            url = "https://#{@host}/api/account/v1/users/#{user_id}/orders/#{order_id}/apps/#{binding_id}"
            response = @access_token.delete(url, { 'Accept'=>'application/json' })
            if response.code == "200"
              update_binding = JSON.parse(response.body)
            else
              raise AppDirectError.new(AppDirectError::APPDIRECT_ERROR_UNBIND, response.code)
            end
          else
            @logger.error("Binding Id, Order Id and User Id are required to cancel a service")
          end
          update_binding
        end

        def cancel_service(user_id, order_id)
          cancel_serv = nil
          if user_id and order_id
            url = "https://#{@host}/api/account/v1/users/#{user_id}/orders/#{order_id}"
            response = @access_token.delete(url, { 'Accept'=>'application/json' })
            if response.code == "200"
              cancel_serv = JSON.parse(response.body)
              @logger.debug("Deleted #{order_id} for #{user_id}")
            else
              raise AppDirectError.new(AppDirectError::APPDIRECT_ERROR_CANCEL, response.code)
            end
          else
            @logger.error("Order Id and User Id are required to cancel a service")
          end
          cancel_serv
        end
      end
    end
  end
end
