# Copyright (c) 2009-2011 VMware, Inc.
require "rest_client"
require "json"

module VCAP
  module Services
    module Rabbit
      module Util

        def add_vhost(vhost)
          @rabbit_resource["vhosts/#{vhost}"].put nil, :content_type => "application/json"
        rescue => e
          @logger.warn(e)
          raise RabbitError.new(RabbitError::RABBIT_ADD_VHOST_FAILED, vhost)
        end

        def delete_vhost(vhost)
          @rabbit_resource["vhosts/#{vhost}"].delete
        rescue => e
          @logger.warn(e)
          raise RabbitError.new(RabbitError::RABBIT_DELETE_VHOST_FAILED, vhost)
        end

        def add_user(username, password)
          @rabbit_resource["users/#{username}"].put "{\"password\":\"#{password}\", \"tags\":\"administrator\"}", :content_type => "application/json"

        rescue => e
          @logger.warn(e)
          raise RabbitError.new(RabbitError::RABBIT_ADD_USER_FAILED, username)
        end

        def delete_user(username)
          @rabbit_resource["users/#{username}"].delete
        rescue => e
          @logger.warn(e)
          raise RabbitError.new(RabbitError::RABBIT_DELETE_USER_FAILED, username)
        end

        def get_permissions_by_options(binding_options)
          # FIXME: binding options is not implemented, use the full permissions.
          @default_permissions
        end

        def get_permissions(vhost, username)
          response = @rabbit_resource["permissions/#{vhost}/#{username}"].get
          JSON.parse(response)
        rescue => e
          @logger.warn(e)
          raise RabbitError.new(RabbitError::RABBIT_GET_PERMISSIONS_FAILED, username)
        end

        def set_permissions(vhost, username, permissions)
          @rabbit_resource["permissions/#{vhost}/#{username}"].put permissions, :content_type => "application/json"
        rescue => e
          @logger.warn(e)
          raise RabbitError.new(RabbitError::RABBIT_SET_PERMISSIONS_FAILED, username, permissions)
        end

        def clear_permissions(vhost, username)
          @rabbit_resource["permissions/#{vhost}/#{username}"].delete
        rescue => e
          @logger.warn(e)
          raise RabbitError.new(RabbitError::RABBIT_CLEAR_PERMISSIONS_FAILED, username)
        end

        def get_vhost_permissions(vhost)
          response = @rabbit_resource["vhosts/#{vhost}/permissions"].get
          JSON.parse(response)
        rescue => e
          @logger.warn(e)
          raise RabbitError.new(RabbitError::RABBIT_GET_VHOST_PERMISSIONS_FAILED, vhost)
        end

        def list_users
          @rabbit_resource["users"].get
        rescue => e
          @logger.warn(e)
          raise RabbitError.new(RabbitError::RABBIT_LIST_USERS_FAILED)
        end

        def list_queues(vhost)
          @rabbit_resource["queues"].get
        rescue => e
          @logger.warn(e)
          raise RabbitError.new(RabbitError::RABBIT_LIST_QUEUES_FAILED, vhost)
        end

        def list_exchanges(vhost)
          @rabbit_resource["exchanges"].get
        rescue => e
          @logger.warn(e)
          raise RabbitError.new(RabbitError::RABBIT_LIST_EXCHANGES_FAILED, vhost)
        end

        def list_bindings(vhost)
          @rabbit_resource["bindings"].get
        rescue => e
          @logger.warn(e)
          raise RabbitError.new(RabbitError::RABBIT_LIST_BINDINGS_FAILED, vhost)
        end

        def close_fds
          3.upto(get_max_open_fd) do |fd|
            begin
              IO.for_fd(fd, "r").close
            rescue
            end
          end
        end

        def get_max_open_fd
          max = 0

          dir = nil
          if File.directory?("/proc/self/fd/") # Linux
            dir = "/proc/self/fd/"
          elsif File.directory?("/dev/fd/") # Mac
            dir = "/dev/fd/"
          end

          if dir
            Dir.foreach(dir) do |entry|
              begin
                pid = Integer(entry)
                max = pid if pid > max
              rescue
              end
            end
          else
            max = 65535
          end

          max
        end

      end
    end
  end
end
