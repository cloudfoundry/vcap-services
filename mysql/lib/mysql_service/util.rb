# Copyright (c) 2009-2011 VMware, Inc.
require "mysql2"
require "monitor"

module VCAP
  module Services
    module Mysql
      module Util
        VALID_CREDENTIAL_CHARACTERS = ("A".."Z").to_a + ("a".."z").to_a + ("0".."9").to_a

        def generate_credential(length=12)
          Array.new(length) { VALID_CREDENTIAL_CHARACTERS[rand(VALID_CREDENTIAL_CHARACTERS.length)] }.join
        end

        def make_logger
          return @logger if @logger
          @logger = Logger.new( STDOUT)
          @logger.level = Logger::DEBUG
          @logger
        end

        def fmt_error(e)
          "#{e}: [#{e.backtrace.join(" | ")}]"
        end

        # dump a single database to the given path
        #  db: the name of the database you want to dump
        #  mysql_config: hash contains following keys:
        #    host, port, user, password and socket as optional
        #  dump_file_path: full file path for dump file
        #  opts : other_options
        #    mysqldump_bin: path of mysqldump binary if not in PATH
        #    gzip_bin: path of gzip binary if not in PATH
        #
        def dump_database(db, mysql_config, dump_file_path, opts={})
          raise ArgumentError, "Missing options." unless db && mysql_config && dump_file_path
          make_logger
          host, user, password, port, socket =  %w{host user pass port socket}.map { |opt| mysql_config[opt] }
          mysql_dump_bin = opts[:mysqldump_bin] || "mysqldump"
          gzip_bin = opts[:gzip_bin] || "gzip"

          socket_str = "-S #{socket}"
          cmd = "#{mysql_dump_bin} -h#{host} -u#{user} -p#{password} -P#{port} #{socket_str if socket} --single-transaction #{db}| #{gzip_bin} - > #{dump_file_path}"
          @logger.info("Take snapshot command:#{cmd}")

          on_err = Proc.new do |cmd, code, msg|
            raise "CMD '#{cmd}' exit with code: #{code}. Message: #{msg}"
          end
          res = CMDHandle.execute(cmd, nil, on_err)
          res
        rescue => e
          @logger.error("Error dump db #{db}: #{fmt_error(e)}")
          nil
        end

        # import data from the dumpfile generated by dump_database
        #  db: the name of the database you want to import
        #  mysql_config: hash contains following keys:
        #    host, port, user, password (root account)and socket as optional
        #  dump_file_path: full file path for dump file
        #  opts : other_options
        #    mysql_bin: path of mysql binary if not in PATH
        #    gzip_bin: path of gzip binary if not in PATH
        #  import_user: the user account used to import db
        #  import_pass: the password used to import db
        def import_dumpfile(db, mysql_config, import_user, import_pass, dump_file_path, opts={})
          raise ArgumentError, "Missing options." unless db && mysql_config && dump_file_path
          make_logger
          host, user, password, port, socket =  %w{host user pass port socket}.map { |opt| mysql_config[opt] }
          mysql_bin = opts[:mysql_bin] || "mysql"
          gzip_bin = opts[:gzip_bin] || "gzip"

          @connection = Mysql2::Client.new(:host => host, :username => user, :password => password, :database => 'mysql' , :port => port.to_i, :socket => socket) unless @connection
          revoke_privileges(db)

          # rebuild database to remove all tables in old db.
          kill_database_session(@connection, db)
          @connection.query("DROP DATABASE #{db}")
          @connection.query("CREATE DATABASE #{db}")
          restore_privileges(db) if @connection

          socket_str = "-S #{socket}"
          cmd = "#{gzip_bin} -dc #{dump_file_path}| #{mysql_bin} -h#{host} -P#{port} -u#{import_user} -p#{import_pass} #{socket_str if socket} #{db}"
          @logger.info("import dump file cmd: #{cmd}")
          on_err = Proc.new do |cmd, code, msg|
            raise "CMD '#{cmd}' exit with code: #{code}. Message: #{msg}"
          end
          res = CMDHandle.execute(cmd, nil, on_err)
          res
        rescue => e
          @logger.error("Failed in import dumpfile to instance #{db}: #{fmt_error(e)}")
          nil
        ensure
          restore_privileges(db) if @connection
        end

        protected
        def revoke_privileges(name)
          @connection.query("UPDATE db SET insert_priv='N', create_priv='N', update_priv='N', lock_tables_priv='N' WHERE Db='#{name}'")
          @connection.query("FLUSH PRIVILEGES")
        end

        def restore_privileges(name)
          @connection.query("UPDATE db SET insert_priv='Y', create_priv='Y', update_priv='Y', lock_tables_priv='Y' WHERE Db='#{name}'")
          @connection.query("FLUSH PRIVILEGES")
        end

        def kill_database_session(connection, database)
          @logger.info("Kill all sessions connect to db: #{database}")
          process_list = connection.query("show processlist")
          process_list.each do |proc|
            thread_id, user, db, command, time, info = proc["Id"], proc["User"], proc["db"], proc["Command"], proc["Time"], proc["Info"]
            if (db == database) and (user != "root")
              connection.query("KILL #{thread_id}")
              @logger.info("Kill session: user:#{user} db:#{db}")
            end
          end
        end

        class ConnectionPool
          include Util
          def initialize(options)
            @options = options
            @timeout = options[:wait_timeout] || 10
            @size = (options[:pool] && options[:pool].to_i) || 5
            @logger = options[:logger] || make_logger
            @connections = []
            @connections.extend(MonitorMixin)
            @cond = @connections.new_cond
            @reserved_connections = {}
            for i in 1..@size do
              @connections << Mysql2::Client.new(@options)
            end
          end

          def with_connection
            connection_id = current_connection_id
            fresh_connection = !@reserved_connections.has_key?(connection_id)
            yield @reserved_connections[connection_id] ||= checkout
          ensure
            release_connection(connection_id) if fresh_connection
          end

          # verify all pooled connections
          def keep_alive
            @connections.synchronize do
              @connections.each_index do |i|
                conn = @connections[i]
                if not conn.ping
                  @logger.debug("Pooled connection #{conn} is dead, try to reconnect.")
                  conn.close
                  @connections[i] = Mysql2::Client.new(@options)
                end
              end
            end
            true
          end

          def close
            @connections.each do |conn|
              conn.close
            end
          end

          def shutdown
            close
            @connections.clear
          end

          # Check the connction with mysql
          def connected?
            keep_alive
          rescue => e
            @logger.warn("Can't connection to mysql: [#{e.errno}] #{e.error}")
            nil
          end

          private
          def release_connection(with_id)
            conn = @reserved_connections.delete(with_id)
            checkin conn if conn
          end

          def clear_stale_cached_connections!
            keys = @reserved_connections.keys - Thread.list.find_all { |t|
              t.alive?
            }.map { |thread| thread.object_id }
            keys.each do |key|
              checkin @reserved_connections[key]
              @reserved_connections.delete(key)
            end
          end

          def checkout
            @connections.synchronize do
              loop do
                conn = @connections.shift
                return verify_connection(conn) if conn

                @cond.wait(@timeout)

                if @connections.empty?
                  next
                else
                  clear_stale_cached_connections!
                  if @connections.empty?
                    raise Mysql2::Error, "could not obtain a database connection#{" within #{@timeout} seconds" if @timeout}.  The max pool size is currently #{@size}; consider increasing it."
                  end
                end
              end
            end
          end

          def verify_connection(conn)
            return nil unless conn
            if not conn.ping
              # reconnect if connection is not active
              @logger.debug("Pooled connection #{conn} is dead, try to reconnect.")
              conn.close
              conn = Mysql2::Client.new(@options)
            end
            conn
          end

          def checkin(conn)
            @connections.synchronize do
              @connections << conn
              @cond.signal
            end
          end

          def current_connection_id
            Thread.current.object_id
          end
        end
      end
    end
  end
end
