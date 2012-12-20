# Copyright (c) 2009-2012 VMware, Inc
require 'pg'
require 'timeout'

module VCAP
  module Services
    module Postgresql
      module Util

        class DBconn
          attr_accessor :query_timeout
          attr_accessor :conn_mutex
          attr_accessor :transaction_tid
          attr_accessor :last_active_time
          attr_accessor :conn

          class << self
            attr_accessor :default_query_timeout
            attr_accessor :default_connect_timeout
            attr_accessor :use_async_query
            attr_accessor :logger
            attr_accessor :error_class
            attr_accessor :query_method
            attr_accessor :async_query_method

            def init(opts={})
              @default_connect_timeout = opts[:db_connect_timeout] || 3
              @default_query_timeout = opts[:db_query_timeout] || 10
              @use_async_query = opts[:db_use_async_query] || false
              @logger = opts[:logger]
            end

            def connect(conn_opts)
              nil
            end

            def async_connect(conn_opts)
              nil
            end

            def async?
              @use_async_query
            end
          end

          def initialize(conn_opts)
            conn_opts[:async] = true if conn_opts[:async].nil?
            @conn = conn_opts[:async] ? self.class.async_connect(conn_opts) : self.class.connect(conn_opts)
            if @conn
              @query_timeout = conn_opts[:query_timeout] || self.class.default_query_timeout
              @conn_mutex = Mutex.new
              @last_active_time = Time.now.to_i
              @conn.query("SET statement_timeout TO #{@query_timeout * 1000}") if @query_timeout
            end
          end

          def nil?
            @conn.nil?
          end

          def active?
            return false unless @conn
            begin
              @conn.query("select 1")
            rescue => e
              false
            end
          end

          def in_transaction?
            Thread.current.object_id == @transaction_tid
          end

          def query_internal(sql)
            @last_active_time = Time.now.to_i
            # add query time out from client side
            if @query_timeout
              begin
                Timeout::timeout(@query_timeout) do
                  async? ? @conn.send(self.class.async_query_method, sql) : @conn.send(self.class.query_method, sql)
                end
              rescue Timeout::Error => e
                raise self.class.error_class, "query timeout (#{@query_timeout} sec) "\
                                              "expired#{async? ? "(sync)" : "(async)"}"
              end
            else
              async? ? @conn.send(self.class.async_query_method, sql) : @conn.send(self.class.query_method, sql)
            end
          end

          def async?
            self.class.async?
          end

          def query(sql)
           if async? && !in_transaction?
              @conn_mutex.synchronize {query_internal(sql) }
            else
              query_internal(sql)
            end
          end

          def transaction(&block)
            @conn.transaction(&block)
          end

          alias :exec :query

          def transaction(&block)
            if async?
              @conn_mutex.synchronize do
                begin
                  @transaction_tid = Thread.current.object_id
                  @conn.transaction(&block)
                ensure
                  self.transaction_tid = nil
                end
              end
            else
              @conn.transaction(&block)
            end
          end

          def close
            @conn.close
          end

          def method_missing(method_name, *args, &block)
            @conn.send(method_name, *args, &block)
          end
        end

        class PGDBconn < DBconn
            class << self
            def init(opts={})
              super(opts)
              @query_method = :query
              @async_query_method = :async_query
              @error_class = PGError
            end

            # blocking and synchronous, may hang whole VM process
            def connect(conn_opts)
              conn_opts[:connect_timeout] ||= self.default_connect_timeout
              PGconn.connect(conn_opts)
            end

            # blocking but asynchronous, won't hang whole VM process
            def async_connect(conn_opts)
              conn_opts[:connect_timeout] ||= self.default_connect_timeout
              conn = PGconn.connect_start(conn_opts)
              raise "Async connect_start failed" if conn.status == PGconn::CONNECTION_BAD
              socket = IO.for_fd(conn.socket)
              socket.autoclose = false
              status = conn.connect_poll
              while status != PGconn::PGRES_POLLING_OK && status != PGconn::PGRES_POLLING_FAILED
                case status
                when PGconn::PGRES_POLLING_READING
                  raise PGError, "Async connect timed out when reading" unless IO.select( [socket], [], [], conn_opts[:connect_timeout] )
                when PGconn::PGRES_POLLING_WRITING
                  raise PGError, "Async connect timed out when writing" unless IO.select( [], [socket], [], conn_opts[:connect_timeout] )
                end
                status = conn.connect_poll
              end
              raise PGError, "Async connect failed for #{conn.error_message}" if status == PGconn::PGRES_POLLING_FAILED
              conn
            end
          end
        end

      end
    end
  end
end
