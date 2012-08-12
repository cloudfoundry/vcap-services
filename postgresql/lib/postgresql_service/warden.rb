$LOAD_PATH.unshift(File.dirname(__FILE__))

module VCAP
  module Services
    module Postgresql
      module Warden
      end
    end
  end
end

module VCAP::Services::Postgresql::Warden

  include VCAP::Services::Postgresql::Util

  def self.included(base)
    unless base.is_a? VCAP::Services::Postgresql::Node
      raise "Warden should be included in a Node instance"
    end
  end

  def pgProvisionedService
    VCAP::Services::Postgresql::Node::Wardenprovisionedservice
  end

  def pgBindUser
    VCAP::Services::Postgresql::Node::Wardenbinduser
  end

  def pre_send_announcement_prepare
    # reset the database
    @connection = postgresql_connect(
      @postgresql_config["host"],
      @postgresql_config["user"],
      @postgresql_config["pass"],
      @postgresql_config["port"],
      @postgresql_config["database"],
      false
    )
    #reset_stats(@connection)
  end

  def pre_send_announcement_internal
    start_instances(pgProvisionedService.all)
    pgProvisionedService.all.each do |provisionedservice|
      migrate_instance provisionedservice
    end
  end

  def migrate_instance provisionedservice
    # TODO
  end

  def global_connection(instance=nil)
    conn = nil
    if instance
      @connections = {} unless @connections
      if instance.is_a?String
        name = instance
      else
        name = instance.name
      end
      conn = @connections[name]
      if conn.nil? || connection_exception(conn)
        instance = pgProvisionedService.get(name) if instance.is_a?String
        return nil unless instance.ip
        conn = @connections[name] = postgresql_connect(
          instance.ip,
          postgresql_config(instance)['user'],
          postgresql_config(instance)['pass'],
          instance.service_port,
          "postgres",
          true
        )
      end
    end
    conn
  end

  def management_connection(instance=nil, super_user=true)
    conn = nil
    if instance.is_a?String
      instance = pgProvisionedService.get(instance)
    end
    if instance
      if super_user
        # use the super user defined in the configuration file
        conn = postgresql_connect(
          instance.ip,
          postgresql_config(instance)['user'],
          postgresql_config(instance)['pass'],
          instance.service_port,
          instance.name,
          true
        )
      else
        # use the default user of the service_instance
        default_user = instance.pgbindusers.all(:default_user => true)[0]
        conn = postgresql_connect(
          instance.ip,
          default_user.user,
          default_user.password,
          instance.service_port,
          instance.name,
          true
        ) if default_user
      end
    end
    conn
  end

  def node_ready?()
    # TODO
    # should check the warden server and postgresql is alive
    true
  end

  #keep connection alive, and check db liveness
  def postgresql_keep_alive
    #TODO
  end

  def get_db_stat
    dbs = []
    pgProvisionedService.all.each do |instance|
      conn = global_connection(instance)
      if conn
        res = get_db_stat_by_connection(conn, @max_db_size)
        dbs += res
      else
        @logger.warn("PostgreSQL connection to #{instance.name}")
      end
    end
    dbs
  end

  def get_db_list
    db_list = []
    pgProvisionedService.all.each do |instance|
      conn = global_connection(instance)
      res = get_db_list_by_connection(conn)
      db_list += res
    end
    db_list
  end

  def dbs_size(dbs=[])
    dbs = [] if dbs.nil?
    result = {}
    dbs.each do |db|
      if db.is_a?pgProvisionedService
        name = db.name
      else
        name= db
      end
      res = global_connection(db).query("select pg_database_size(datname) as sum_size from pg_database where datname = '#{name}'")
      res.each do |x|
        size = x["sum_size"]
        result[name] = size.to_i
      end
    end
    result
  end

  def postgresql_config(instance=nil)
    unless instance && instance.is_a?(pgProvisionedService) && instance.name
      @postgresql_config
    else
      pc = @postgresql_config.dup
      pc['name'] = instance.name
      pc['host'] = instance.ip
      pc['port'] = instance.service_port
      pc
    end
  end

  def kill_long_queries
    pgProvisionedService.all.each do |service|
      @long_queries_killed += kill_long_queries_internal(global_connection(service), postgresql_config(service)['user'], @max_long_tx)
    end
  rescue => e
    @logger.warn("PostgreSQL Node exception: " + fmt_error(e))
  end

  def kill_long_transaction
    pgProvisionedService.all.each do |service|
      @long_tx_killed += kill_long_transaction_internal(global_connection(service), postgresql_config(service)['user'], @max_long_tx)
    end
  rescue => e
    @logger.warn("PostgreSQL Node exception: " + fmt_error(e))
  end

  def setup_timers
    EM.add_periodic_timer(@max_long_query.to_f / 2) {kill_long_queries} if @max_long_query > 0
    EM.add_periodic_timer(@max_long_tx.to_f / 2) {kill_long_transaction} if @max_long_tx > 0
    EM.add_periodic_timer(VCAP::Services::Postgresql::Node::STORAGE_QUOTA_INTERVAL) {enforce_storage_quota}
  end

  def shutdown
    super
    @logger.info("Shutting down instances..")
    pgProvisionedService.all.each do |instance|
      @logger.debug('Try to terminate postgresql container:#{instance.container}')
      instance.stop if instance.running?
    end
  end

  def get_inst_port(instance=nil)
    (instance.port if instance) || @postgresql_config['port']
  end

  def free_inst_port(port)
    free_port(port)
  end

  def set_inst_port(instance, credential)
    @logger.debug("Will reuse the port #{credential['port']}") if credential
    instance.port = new_port((credential['port'] if credential))
  end

  def is_service_started(instance)
    global_connection(instance).nil? ? false : true
  end
end
