# Copyright (c) 2009-2011 VMware, Inc.

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..")
require "node"
require "util"
require "postgresql_error"

module VCAP::Services::Snapshot::Postgresql
  include VCAP::Services::Snapshot

  # Dump a database into files and save the snapshot information into redis.
  class CreateSnapshotJob < SnapshotJob

    include VCAP::Services::Postgresql::Util

    # Resque 's job must implement the perform method
    def perform
      # service id is the provisioned service's id
      name = options["service_id"]
      @logger.info("Begin create snapshot job for: #{name}")

      # get the snapshot id
      VCAP::Services::Snapshot.redis_connect(@config["resque"])
      snapshot_id = get_snapshot_id

      # dump the db and get the dump file size
      dump_file_size = dump_db(name, snapshot_id)

      # gather the information of the snapshot
      complete_time = Time.now
      snapshot = {
        :snapshot_id => snapshot_id,
        :date => complete_time.to_s,
        :size => dump_file_size
      }

      # save the sanpshot info
      save_snapshot(name, snapshot)
      job_result = { :snapshot_id => snapshot_id }
      set_status({ :complete_time=> complete_time.to_s  })
      completed(Yajl::Encoder.encode(job_result))

    rescue => e
      @logger.error("Error in CreateSnapshotJob #{@uuid}:#{fmt_error(e)}")
      cleanup(name, snapshot_id)
      err = (e.instance_of?(ServiceError)? e: ServiceError.new(ServiceError::INTERNAL_ERROR)).to_hash
      err_msg = Yajl::Encoder.encode(err)
      set_status({ :complete_time => Time.now.to_s })
      failed(err_msg)
    end

    def dump_db(name, snaptshot_id)
      # dump file
      dump_path = get_dump_path(name, snapshot_id)
      FileUtils.mkdir_p(dump_path) unless File.exists?(full_path)
      dump_file_name = File.join(dump_path, "#{snapshot_id}.dump")

      # postgresql's config
      postgre_conf = @config['postgresql']

      # setup DataMapper
      VCAP::Services::Postgresql::Node.setup_datamapper(:default, @config['local_db'] )
      # prepare the command
      provisionedservice = VCAP::Services::Postgresql::Node::Provisionedservice.get(name)
      default_user = provisionedservice.bindusers.all(:default_user => true)[0]
      if default_user.nil?
        @logger.error("The provisioned service with name #{name} has no default user")
        raise "Failed to dump database of #{name}"
      end
      user = default_user[:user]
      passwd = default_user[:password]
      host, port = %{ host, port }.map{ |k| postgre_conf[k] }

      # dump the database
      dump_databse(name, host, port, user, passwd, dump_file_name ,{ :dump_bin => @config["dump_bin"], :logger => @logger})
      dump_file_size = -1
      File.open(dump_file_name) { |f| dump_file_size = f.size }
      # we will return the dump file size
      dump_file_size
    end
  end

  # Rollback data from snapshot files
  class RollbackSnapshotJob < SnapshotJob

    include VCAP::Services::Postgresql::Util

    def perform
      name = options["service_id"]
      snapshot_id = options["snapshot_id"]
      @logger.info("Begin to rollback snapshot #{snapshot_id} job for #{name} ")

      # try to restore the data
      result = restore_db(name, snapshot_id)
      set_status({ :complete_time => Time.now.to_s })
      completed(Yajl::Encoder.encode( {:result => "ok" } ))

    rescue => e
      @logger.error("Error in Rollback snapshot job #{@uuid}:#{fmt_error(e)}")
      err = ( e.instance_of?(ServiceError)? e: ServiceError.new(ServiceError::INTERNAL_ERROR)).to_hash
      err_msg = Yajl::Encder.encode(err)
      set_status(:Complete_time => Time.now.to_s)
      failed(err_msg)
    end

    def restore_db(name, snapshot_id)

      VCAP::Services::Postgresql::Node.setup_datamapper(:default, @config["local_db"])
      service = VCAP::Services::Postgresql::Node::Provisionedservice.get(name)
      raise "No information for provisioned service with name #{name}." unless service
      default_user = service.bindusers.all(:default_user => true)[0]
      raise "No default user for service #{name}." unless default_user

      dump_path = get_dump_path(name, snapshot_id)
      dump_file_name = File.join( dump_path, "#{snapshot_id}.dump" )
      raise "Can't find snapshot file #{snapshot_file_path}" unless File.exists?(dump_file_name)

      host, port, vcap_user, vcap_pass = %w{ host, port, user, pass }.map{ |k| @config["postgresql"][k]}

      # Need a user who is a superuser to disable db access and then kill all live sessions first

      pgconn = PGConn(host, port, nil, nil, "postgres", vcap_user, vcap_pass)

      disable_db_conn(pgconn, db, service)

      kill_sessions_conn(pgconn, db)

      db_info = get_db_info(pgconn, db)

      drop_db(pgconn, db)
      create_db(pgconn, db, db_info)

      dbconn.PGConn(host, port, nil, nil, name, vcap_user, vcap_pass)
      grant_user_priv(dbconn)
      dbconn.close

      enable_db_conn(pgconn, db, service)

      pgconn.close

      # Import the dump file
      parent_user = default_user[:user]
      parent_passwd = default_user[:password]
      restore_bin = @config["restore_bin"]

      restore_database(name, host, port, parent_user, parent_passwd, dump_file_name, { :resotre_bin => restore_bin, :logger => @logger } )
    end

    def grant_user_prvi(conn)
        return unless conn && db
        conn.query("grant create on schema public to public")
        if pg_version(conn) == '9'
          conn.query("grant all on all tables in schema public to public")
          conn.query("grant all on all sequences in schema public to public")
          conn.query("grant all on all functions in schema public to public")
        else
          querys = conn.query("select 'grant all on '||tablename||' to public;' as query_to_do from pg_tables where schemaname = 'public'")
          querys.each do |query_to_do|
            p query_to_do['query_to_do'].to_s
            conn.query(query_to_do['query_to_do'].to_s)
          end
          querys = conn.query("select 'grant all on sequence '||relname||' to public;' as query_to_do from pg_class where relkind = 'S'")
          querys.each do |query_to_do|
            conn.query(query_to_do['query_to_do'].to_s)
           end
        end
    end

    def db_info(conn, db)
      return unless conn
      conn.query("select * from pg_db where datname=#{db} ")
    end

    def drop_db(conn, db)
      return unless conn
      conn.query("drop database #{db}")
    end

    def create_db(pgconn, db)
      return unless conn
      conn.query("drop database #{db}")
    end

    def create_db(pgconn, db, db_info)
      return unless conn
      if db_info["max_connection_limit"].nil?
        conn.query("create database #{db} with connection_limit = #{db_info["max_connection_limit"]}")
      else
        conn.query("create database #{db}")
      end
    end

    def kill_alive_sessions(conn, db)
      return unless conn
      @logger.info("Kill all alive sessions connect to db: #{db}")
      @conn.query("select pg_terminate_backend(procpid) from pg_stat_activity where datname = #{db}")
    end

    def disable_db_conn(conn, db, service)
      return unless conn && service
      service.bindusers.each do |binduser|
        conn.query("revoke connect on database #{db} from #{binduser.user}")
        conn.query("revoke connect on database #{db} from #{binduser.sysuser}")
      end
    end

    def enable_db_conn(conn, db, service)
      return unless conn && service
      service.bindusers.each do |binduser|
        conn.query("grant connect on database #{db} from #{binduser.user}")
        conn.query("grant connect on database #{db} from #{binduser.sysuser}")
      end
    end
  end
end
