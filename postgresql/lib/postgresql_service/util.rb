# Copyright (c) 2009-2011 VMware, Inc.
require 'pg'

module VCAP
  module Services
    module Postgresql

      # Give various helper functions
      module Util
        VALID_CREDENTIAL_CHARACTERS = ("A".."Z").to_a + ("a".."z").to_a + ("0".."9").to_a

        include VCAP::Services::Base::Utils

        def fmt_error(e)
          "#{e}: [#{e.backtrace.join(" | ")}]"
        end

        def create_logger(logdev, rotation, level)
          if String === logdev
            dir = File.dirname(logdev)
            FileUtils.mkdir_p(dir) unless File.directory?(dir)
          end
          logger = Logger.new(logdev, rotation)
          logger.level = case level
            when "DEBUG" then Logger::DEBUG
            when "INFO" then Logger::INFO
            when "WARN" then Logger::WARN
            when "ERROR" then Logger::ERROR
            when "FATAL" then Logger::FATAL
            else Logger::UNKNOWN
          end
          logger
        end

        def generate_credential(length=12)
          Array.new(length) { VALID_CREDENTIAL_CHARACTERS[rand(VALID_CREDENTIAL_CHARACTERS.length)] }.join
        end

        # shell CMD wrapper and logger
        def exe_cmd(cmd, env={}, stdin=nil)
          @logger ||= create_logger
          @logger.debug("Execute shell cmd:[#{cmd}]")
          o, e, s = Open3.capture3(env, cmd, :stdin_data => stdin)
          if s.exitstatus == 0
            @logger.info("Execute cmd:[#{cmd}] succeeded.")
          else
            @logger.error("Execute cmd:[#{cmd}] failed. Stdin:[#{stdin}], stdout: [#{o}], stderr:[#{e}]")
          end
          return [o, e, s]
        end

        # Return the version of postgresql
        def pg_version(conn)
          return '-1' unless conn
          version = conn.query("select version()")
          reg = /([0-9.]{5})/
          return version[0]['version'].scan(reg)[0][0][0]
        end

        # Return the public schema id of postgresql
        def get_public_schema_id(conn)
          if conn
            res = conn.query("select oid, nspname, nspowner from pg_namespace where nspname = 'public'")
            schema_id = nil
            res.each do |nsp|
              schema_id = nsp['oid']
              break
            end
            schema_id
          else
            nil
          end
        end

        def postgresql_quickcheck(host, user, password, port, database)
          ret = true
          begin
            connect = PGconn.connect(host, port, nil, nil, database, user, password)
            if connect
              pg_version(connect)
              connect.close
            else
              ret = false
            end
          rescue => e
            ret = false
          end
          ret
        end

        def postgresql_connect(host, user, password, port, database, fail_with_nil = false, connect_timeout = 3, try_num = 5, exception_sleep = 2)
          @logger ||= Logger.new(STDOUT)
          try_num.times do
            begin
              @logger.info("PostgreSQL connect: #{host}, #{port}, #{user}, #{password}, #{database} (fail_with_nil: #{fail_with_nil})")
              connect = PGconn.connect(
                :host => host,
                :port => port,
                :options => nil,
                :tty => nil,
                :dbname => database,
                :user => user,
                :password => password,
                :connect_timeout => connect_timeout)
              version = pg_version(connect)
              @logger.info("PostgreSQL server version: #{version}")
              @logger.info("Connected")
              return connect
            rescue PGError => e
              @logger.error("PostgreSQL connection attempt failed: #{host} #{port} #{database} #{user} #{password}")
              sleep(exception_sleep)
            end
          end
          if fail_with_nil
            @logger.warn("PostgreSQL connection unrecoverable")
            return nil
          else
            @logger.fatal("PostgreSQL connection unrecoverable")
            shutdown if self.respond_to?(:shutdown)
            exit
          end
        end

        # Return all schemas owned by current logined user
        def get_conn_schemas(default_connection)
          if default_connection
            schemas = {}
            res = default_connection.query("select n.oid as nspid,n.nspname,n.nspowner from pg_namespace as n inner join pg_roles as r on n.nspowner = r.oid where r.rolname = '#{default_connection.user}'")
            res.each do |ns|
              schemas[ns['nspname']] = ns['nspid']
            end
            schemas
          else
            nil
          end
        end

        # Alter owner of database
        def reset_owner(pgconn, name, owner)
          return unless pgconn
          pgconn.query("alter database #{name} owner to #{owner}")
        end

        # Legacy method to alter owner of relationship from sys_user to user
        def do_grant_query(db_connection,user,sys_user)
          return unless db_connection
          db_connection.query("update pg_class set relowner = (select oid from pg_roles where rolname = '#{user}') where relowner = (select oid from pg_roles where rolname = '#{sys_user}')")
        end

        # Legacy method to revoke privileges of public shcema
        def do_revoke_query(db_connection, user, sys_user)
          db_connection.query("revoke create on schema public from #{user} CASCADE")
          if pg_version(db_connection) == '9'
            db_connection.query("REVOKE ALL ON ALL TABLES IN SCHEMA PUBLIC from #{user} CASCADE")
            db_connection.query("REVOKE ALL ON ALL SEQUENCES IN SCHEMA PUBLIC from #{user} CASCADE")
            db_connection.query("REVOKE ALL ON ALL FUNCTIONS IN SCHEMA PUBLIC from #{user} CASCADE")
          else
            queries = db_connection.query("select 'REVOKE ALL ON '||tablename||' from #{user} CASCADE;' as query_to_do from pg_tables where schemaname = 'public'")
            queries.each do |query_to_do|
               db_connection.query(query_to_do['query_to_do'].to_s)
            end
            queries = db_connection.query("select 'REVOKE ALL ON SEQUENCE '||relname||' from #{user} CASCADE;' as query_to_do from pg_class where relkind = 'S'")
            queries.each do |query_to_do|
               db_connection.query(query_to_do['query_to_do'].to_s)
            end
          end

          # with the fix for user access rights in r8, actually this line is a no-op.
          # - for newly created users(after the fix), all objects created will be owned by parent
          # - for existing users(created before the fix), if quota exceeds, then sys_user will
          #  own the objects, but, when the fix comes, the migration job will pull all the objects
          #  (both user and sys_user) to parent as the owner. So, after the fix comes, there is no
          #  object owned by sys_user.
          # while quota can be still enforced because 'revoke_write_access' and 'do_revoke_query'
          # do the work.
          db_connection.query("update pg_class set relowner = (select oid from pg_roles where rolname = '#{sys_user}') where relowner = (select oid from pg_roles where rolname ='#{user}')")
        end

        # Legacy method to grant user privileges of public schema
        def exe_grant_user_priv(conn)
        @logger ||= create_logger
        unless conn
          @logger.error("No connection to do exe_grant_user_priv")
          return
        end
        grant_user_priv(conn, pg_version(conn))
        end
        def grant_user_priv(conn, version)
          return unless conn
          conn.query("grant create on schema public to public")
          if version == '9'
            conn.query("grant all on all tables in schema public to public")
            conn.query("grant all on all sequences in schema public to public")
            conn.query("grant all on all functions in schema public to public")
          else
            queries = conn.query("select 'grant all on '||tablename||' to public;' as query_to_do from pg_tables where schemaname = 'public'")
            queries.each do |query_to_do|
              conn.query(query_to_do['query_to_do'].to_s)
            end
            queries = conn.query("select 'grant all on sequence '||relname||' to public;' as query_to_do from pg_class where relkind = 'S'")
            queries.each do |query_to_do|
              conn.query(query_to_do['query_to_do'].to_s)
            end
          end
        end

        # Grant write access privileges to role on schema
        def grant_schema_write_access(db_connection, schema_id, schema, role)
          return unless db_connection
          db_connection.query("grant create on schema #{schema} to #{role}")
          if pg_version(db_connection) == '9'
            db_connection.query("grant all on all tables in schema #{schema} to #{role}")
            db_connection.query("grant all on all sequences in schema #{schema} to #{role}")
            db_connection.query("grant all on all functions in schema #{schema} to #{role}")
          else
            queries = db_connection.query("select 'grant all on #{schema}.'||tablename||' to #{role};' as query_to_do from pg_tables where schemaname = '#{schema}'")
            queries.each do |query_to_do|
              db_connection.query(query_to_do['query_to_do'].to_s)
            end
            queries = db_connection.query("select 'grant all on sequence #{schema}.'||relname||' to #{role};' as query_to_do from pg_class where relkind = 'S' and relnamespace = #{schema_id}")
            queries.each do |query_to_do|
              db_connection.query(query_to_do['query_to_do'].to_s)
            end
          end
        end

        # Revoke write access privileges from role on schema
        def revoke_schema_write_access(db_connection, schema_id, schema, role)
          return unless db_connection
          db_connection.query("revoke create on schema #{schema} from #{role} CASCADE")
          if pg_version(db_connection) == '9'
            db_connection.query("REVOKE ALL ON ALL TABLES IN SCHEMA #{schema} from #{role} CASCADE")
            db_connection.query("REVOKE ALL ON ALL SEQUENCES IN SCHEMA #{schema} from #{role} CASCADE")
            db_connection.query("REVOKE ALL ON ALL FUNCTIONS IN SCHEMA #{schema} from #{role} CASCADE")
            db_connection.query("grant select,delete,truncate,references,trigger on all tables in schema #{schema} to #{role}")
            db_connection.query("grant usage,select on all sequences in schema #{schema} to #{role}")
          else
            queries = db_connection.query("select 'REVOKE ALL ON #{schema}.'||tablename||' from #{role} CASCADE;' as query_to_do from pg_tables where schemaname = '#{schema}'")
            queries.each do |query_to_do|
               db_connection.query(query_to_do['query_to_do'].to_s)
            end
            # revoke privileges of sequence should belong to the schema
            queries = db_connection.query("select 'REVOKE ALL ON SEQUENCE #{schema}.'||relname||' from #{role} CASCADE;' as query_to_do from pg_class where relkind = 'S' and relnamespace = #{schema_id}")
            queries.each do |query_to_do|
               db_connection.query(query_to_do['query_to_do'].to_s)
            end
            queries = db_connection.query("select 'grant select,delete,truncate,references,trigger on #{schema}.'||tablename||' to #{role};' as query_to_do from pg_tables where schemaname = '#{schema}'")
            queries.each do |query_to_do|
               db_connection.query(query_to_do['query_to_do'].to_s)
            end
            queries = db_connection.query("select 'grant usage,select on SEQUENCE #{schema}.'||relname||' to #{role};' as query_to_do from pg_class where relkind = 'S' and relnamespace = #{schema_id}")
            queries.each do |query_to_do|
               db_connection.query(query_to_do['query_to_do'].to_s)
            end
          end
        end

        # Grant write access privilege of database
        def grant_write_access_internal(db_connection, service, public_schema_id=nil)
          return false unless db_connection && service
          @logger ||= Logger.new(STDOUT)
          name = service.name
          default_user = service.default_user
          unless default_user
            @logger.error("No default user #{default_user} for database #{name} when granting write access")
            return false
          end
          default_connection = postgresql_connect(db_connection.host, default_user[:user], default_user[:password], db_connection.port, name, true)
          unless default_connection
            @logger.error("Default user failed to connect to database #{name} when granting write access")
            return false
          end
          public_schema_id ||= get_public_schema_id(db_connection)
          unless public_schema_id
            @logger.error("Fail to get public schema id")
            return false
          end
          service.pgbindusers.all.each do |binduser|
            user = binduser.user
            sys_user = binduser.sys_user
            sys_password = binduser.sys_password
            db_connection_sys_user = postgresql_connect(db_connection.host, sys_user, sys_password, db_connection.port, name, true)
            if db_connection_sys_user.nil?
              @logger.error("Unable to grant write access to #{name} for #{sys_user}")
            else
              db_connection_sys_user.query("vacuum full")
              db_connection_sys_user.close
              do_grant_query(db_connection, user, sys_user)
            end
            db_connection.query("GRANT TEMP ON DATABASE #{name} to #{user}")
            db_connection.query("GRANT TEMP ON DATABASE #{name} to #{sys_user}")
          end
          grant_schema_write_access(db_connection, public_schema_id, 'public', 'public')
          schemas = get_conn_schemas(default_connection) || {}
          schemas.each do |sc, sc_id|
            grant_schema_write_access(default_connection, sc_id, sc, default_user[:user])
          end

          db_connection.query("grant create on database #{name} to #{default_user[:user]}")
          service.quota_exceeded = false
          service.save
          true
        end

        # Revoke write access privileges of database
        def revoke_write_access_internal(pgconn, db_connection, service, public_schema_id=nil)
          return false unless pgconn && db_connection && service
          @logger ||= Logger.new(STDOUT)
          name = service.name
          default_user = service.default_user
          unless default_user
            @logger.warn("No default user #{default_user} for database #{name} when granting write access")
            return false
          end
          default_connection = postgresql_connect(db_connection.host, default_user[:user], default_user[:password], db_connection.port, name, true)
          unless default_connection
            @logger.warn("Default user #{default_user} fail to connect to database #{name} when revoking write access")
            return false
          end
          public_schema_id ||= get_public_schema_id(db_connection)
          unless public_schema_id
            @logger.warn("Fail to get public schema id")
            return false
          end
          # revoke create privilege from database
          db_connection.query("revoke create on database #{name} from #{default_user[:user]}")

          # revoke write access from public shema
          revoke_schema_write_access(db_connection, public_schema_id, 'public', 'public')

          # revoke write privilege on all created schemas on the database
          # only the members in the same group could see the schemas, even super user could not see them
          schemas = get_conn_schemas(default_connection) || {}
          schemas.each do |sc, sc_id|
            revoke_schema_write_access(default_connection, sc_id, sc, default_user[:user])
          end
          default_connection.close if default_connection

          # revoke temp privilege on the database
          service.pgbindusers.all.each do |binduser|
            user = binduser.user
            sys_user = binduser.sys_user
            kill_alive_sessions(pgconn, name, user)
            db_connection.query("REVOKE TEMP ON DATABASE #{name} from #{user}")
            db_connection.query("REVOKE TEMP ON DATABASE #{name} from #{sys_user}")
            do_revoke_query(db_connection, user, sys_user)
          end
          service.quota_exceeded = true
          service.save
          true
        end

        # Return information of database
        def get_db_info(conn, db)
          return unless conn
          result = conn.query("select * from pg_database where datname='#{db}'")
          result[0]
        end

        # Drop database
        def exe_drop_database(conn, name)
          @logger ||= create_logger
          unless conn
            @logger.warn("No connection to drop database #{name}")
            return
          end
          @logger.info("Deleting database: #{name}")
          begin
            conn.query("select pg_terminate_backend(procpid) from pg_stat_activity where datname = '#{name}'")
          rescue PGError => e
            @logger.warn("Could not kill database session: #{e}")
          end
          drop_db(conn, name)
        end

        def drop_db(conn, db)
          return unless conn
          conn.query("drop database #{db}")
        end

        # Create database
        def exe_create_database(conn, name, max_db_conns)
          @logger ||= create_logger
          unless conn
            @logger.warn("No connection to create database #{name}")
            return
          end
          @logger.debug("Maximum connections: #{max_db_conns}")
          db_info = {}
          db_info["datconnlimit"] = max_db_conns if max_db_conns
          create_db(conn, name, db_info)
        end

        def create_db(conn, db, db_info)
          return unless conn
          if db_info["datconnlimit"]
            conn.query("create database #{db} with connection limit = #{db_info["datconnlimit"]}")
          else
            conn.query("create database #{db}")
          end
          conn.query("revoke all on database #{db} from public")
        end

        # Interrupt all activities on database
        def kill_alive_sessions(conn, db, user=nil)
          return unless conn
          unless user
            conn.query("select pg_terminate_backend(procpid) from pg_stat_activity where datname='#{db}'")
          else
            conn.query("select pg_terminate_backend(procpid) from pg_stat_activity where datname='#{db}' and usename = '#{user}'")
          end
        end

        # Block all binding users to connect the database
        def block_user_from_db(db_connection, service)
          name = service.name
          default_user = service.default_user
          service.pgbindusers.all.each do |binduser|
            if binduser.default_user == false
              db_connection.query("revoke #{default_user[:user]} from #{binduser.user}")
              db_connection.query("revoke connect on database #{name} from #{binduser.user}")
              db_connection.query("revoke connect on database #{name} from #{binduser.sys_user}")
            end
          end
        end

        # Permit all binding usrs to connect the database
        def unblock_user_from_db(db_connection, service)
          name = service.name
          default_user = service.default_user
          service.pgbindusers.all.each do |binduser|
            if binduser.default_user == false
              db_connection.query("GRANT CONNECT ON DATABASE #{name} to #{binduser.user}")
              db_connection.query("GRANT CONNECT ON DATABASE #{name} to #{binduser.sys_user}")
              db_connection.query("GRANT #{default_user[:user]} to #{binduser.user}")
            end
          end
        end

        # Block all users to connec the database
        def disable_db_conn(conn, db, service)
          return unless conn && service
          service.pgbindusers.each do |binduser|
            conn.query("revoke connect on database #{db} from #{binduser.user}")
            conn.query("revoke connect on database #{db} from #{binduser.sys_user}")
          end
        end

        # Enable all users to connect the dtabase
        def enable_db_conn(conn, db, service)
          return unless conn && service
          service.pgbindusers.each do |binduser|
            conn.query("grant connect on database #{db} to #{binduser.user}")
            conn.query("grant connect on database #{db} to #{binduser.sys_user}")
          end
        end

        # Check whether a connection is alive
        def connection_exception(conn)
          conn.query("select current_timestamp")
          return nil
        rescue => e
          @logger ||= create_logger
          @logger.warn("PostgreSQL connection #{(conn.inspect if conn)} lost: #{e}")
          return e
        end

        # Drop the database and re-create it for restoring/rolling back
        def reset_db(host, port, vcap_user, vcap_pass, name, service)
          pgconn = PGconn.new(host, port, nil, nil, "postgres", vcap_user, vcap_pass)
          disable_db_conn(pgconn, name, service)
          kill_alive_sessions(pgconn, name)
          db_info = get_db_info(pgconn, name)
          db_version = pg_version(pgconn)
          # we should considering re-set the privileges (such as create/temp ...) for parent role
          drop_db(pgconn, name)

          create_db(pgconn, name, db_info)
          enable_db_conn(pgconn, name, service)

          # should re-grant write privilege on database to parent role for restoring schemas
          # at the same time, for the database is recreated, the size should be under quota, it is safe to do this.
          # service.exceeded_quota should be false after this.
          dbconn = PGconn.new(host, port, nil, nil, name, vcap_user, vcap_pass)
          unless grant_write_access_internal(dbconn, service)
            raise "Fail to grant write access when reseting the database #{name}"
          end
        ensure
          dbconn.close if dbconn
          pgconn.close if pgconn
        end

        # Use this method for backuping and snapshoting the database
        # name: name of database
        # host: ip/hostname of your database node
        # port: port of your database listening
        # user
        # passwd
        # dump_file: the file to store the dumped data
        # opts: optional arguments
        #   dump_bin
        #   logger
        def dump_database(name, host, port, user, passwd, dump_file, opts = {})
          raise "You must provide the following arguments: name, host, port, user, passwd, dump_file" unless name && host && port && user && passwd && dump_file

          dump_bin = opts[:dump_bin] || 'pg_dump'
          dump_cmd = "#{dump_bin} -Fc --host=#{host} --port=#{port} --username=#{user} --file=#{dump_file} #{name}"

          # running the command
          on_err = Proc.new do |cmd, code, msg|
            opts[:logger].error("CMD '#{cmd}' exit with code: #{code} & Message: #{msg}") if opts[:logger]  && opts[:logger].respond_to?(:error)
          end

          result = CMDHandle.execute(dump_cmd, nil, on_err )
          raise "Failed to dump database of #{name}" unless result
          result
        end

        # Use this method to filter the un-supported archive elements in HACK style
        def archive_list(dump_file, opts = {})
          restore_bin = opts[:restore_bin] || 'pg_restore'
          cmd = "#{restore_bin} -l #{dump_file} | grep -v 'PROCEDURAL LANGUAGE - plpgsql' > #{dump_file}.archive_list"
          o, e, s = exe_cmd(cmd)
          return s.exitstatus == 0
        end

        # Use this method for restoring and importing the database
        # name: name of database
        # host: ip/hostname of your database node
        # port: port of your database listening
        # user
        # passwd
        # dump_file: the file which stores the dumped data
        # opts: optional arguments
        #   restore_bin
        #   logger
        def restore_database(name, host, port, user, passwd, dump_file, opts = {})
          raise "You must provide the following arguments: name, host, port, user, passwd, dump_file" unless name && host && port && user && passwd && dump_file

          archive_list(dump_file, opts)

          restore_bin = opts[:restore_bin] || 'pg_restore'
          restore_cmd = "#{restore_bin} -h #{host} -p #{port} -U #{user} -L #{dump_file}.archive_list -d #{name} #{dump_file} "

          # running the command
          o, e, s = exe_cmd(restore_cmd)
          s.exitstatus == 0
        ensure
          FileUtils.rm_rf("#{dump_file}.archive_list")
        end

        def is_default_bind_user(user_name)
          if respond_to?(:pgBindUser)
            user = pgBindUser.get(user_name)
            !user.nil? && user.default_user
          else
            return false
          end
        end

        def kill_long_queries_internal(connection, super_user, max_long_query)
          @logger ||= create_logger
          long_queries_killed = 0
          unless connection && super_user && max_long_query
            @logger.warn("Invalid parameters to kill long queries: #{connection}, #{super_user}, #{max_long_query}")
            return long_queries_killed
          end

          begin
            # (extract(epoch from current_timestamp) - extract(epoch from query_start)) as runtime
            # Notice: we should use current_timestamp or timeofday, the difference is that the current_timestamp only executed once at the beginning of the transaction, while dayoftime will return a text string of wall-clock time and advances during the transaction
            # Filtering the long queries in the pg statement is better than filtering using the iteration of ruby after select all activties
            process_list = connection.query("select * from (select procpid, datname, query_start, usename, (extract(epoch from current_timestamp) - extract(epoch from query_start)) as run_time from pg_stat_activity where query_start is not NULL and usename != '#{super_user}' and current_query !='<IDLE>') as inner_table  where run_time > #{max_long_query}")
            process_list.each do |proc|
              unless is_default_bind_user(proc["usename"])
                connection.query("select pg_terminate_backend(#{proc['procpid']})")
                @logger.info("Killed long query: user:#{proc['usename']} db:#{proc['datname']} time:#{Time.now.to_i - Time::parse(proc['query_start']).to_i} info:#{proc['current_query']}")
                long_queries_killed += 1
              end
            end
          rescue PGError => e
            @logger.warn("PostgreSQL error: #{e}")
          end
          long_queries_killed
        end

        def kill_long_transaction_internal(connection, super_user, max_long_tx)
          @logger ||= create_logger
          long_tx_killed = 0
          unless connection && super_user && max_long_tx
            @logger.warn("Invalid parameters to kill long tx: #{connection}, #{super_user}, #{max_long_tx}")
            return long_tx_killed
          end
          begin
            # see kill_long_queries
            process_list = connection.query("select * from (select procpid, datname, xact_start, usename, (extract(epoch from current_timestamp) - extract(epoch from xact_start)) as run_time from pg_stat_activity where xact_start is not NULL and usename != '#{super_user}') as inner_table where run_time > #{max_long_tx}")
            process_list.each do |proc|
              unless is_default_bind_user(proc["usename"])
                connection.query("select pg_terminate_backend(#{proc['procpid']})")
                @logger.info("Killed long transaction: user:#{proc['usename']} db:#{proc['datname']} active_time:#{Time.now.to_i - Time::parse(proc['xact_start']).to_i}")
                long_tx_killed += 1
              end
            end
          rescue PGError => e
            @logger.warn("PostgreSQL error: #{e}")
          end
          long_tx_killed
        end

        def get_db_stat_by_connection(connection, max_db_size)
          @logger ||= create_logger()
          sys_dbs = ['template0', 'template1', 'postgres']
          result = []
          return result unless connection
          db_stats = connection.query('select datid, datname, version() as version from pg_stat_database')
          db_stats.each do |d|
            name = d["datname"]
            oid = d["datid"]
            version = d["version"]
            next if sys_dbs.include?(name)
            db = {}
            # db name
            db[:name] = name
            # db verison
            db[:version] = version
            # db max size
            db[:max_size] = max_db_size
            # db actual size
            sizes = connection.query("select pg_database_size('#{name}')")
            db[:size] = sizes[0]['pg_database_size'].to_i
            # db active connections
            a_s_ps = connection.query("select pg_stat_get_db_numbackends(#{oid})")
            db[:active_server_processes] = a_s_ps[0]['pg_stat_get_db_numbackends'].to_i
            result << db
          end
          result
        rescue => e
          @logger.warn("Error during generate varz/db_stat: #{e}")
          []
        end

        def get_db_list_by_connection(connection)
          @logger ||= create_logger
          db_list = []
          return db_list unless connection
          connection.query('select datname,datacl from pg_database').each{ |message|
            datname = message['datname']
            datacl = message['datacl']
            if not datacl==nil
              users = datacl[1,datacl.length-1].split(',')
              for user in users
                if user.split('=')[0].empty?
                else
                  db_list.push([datname, user.split('=')[0]])
                end
              end
            end
          }
          db_list
        rescue => e
          @logger.error("Fail to get db list using connection #{connection} for #{fmt_error(e)}")
          []
        end

      end
    end
  end
end
