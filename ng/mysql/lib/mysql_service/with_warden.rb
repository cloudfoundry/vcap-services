module VCAP
  module Services
    module Mysql
      module WithWarden
      end
    end
  end
end

module VCAP::Services::Mysql::WithWarden
  def mysqlProvisionedService
    VCAP::Services::Mysql::Node::WardenProvisionedService
  end

  def init_internal(options)
    @service_start_timeout = @options[:service_start_timeout] || 3
    init_ports(options[:port_range])
  end

  def pre_send_announcement_internal
    @pool_mutex = Mutex.new
    @pools = {}

    @capacity_lock.synchronize do
      start_instances(mysqlProvisionedService.all)
    end

    mysqlProvisionedService.all.each do |instance|
      setup_pool(instance)
    end
  end

  def handle_provision_exception(provisioned_service)
    return unless provisioned_service
    free_port(provisioned_service.port)
    provisioned_service.delete
  end

  def get_port(provisioned_service)
    provisioned_service.port
  end

  def help_unprovision(provisioned_service)
    name = provisioned_service.name
    @pool_mutex.synchronize do
      @pools[name].shutdown
      @pools.delete(name)
    end
    free_port(provisioned_service.port)
    raise "Could not cleanup instance #{provisioned_service.name}" unless provisioned_service.delete
  end

  def is_service_started(instance)
    get_status(instance) == "ok"
  end

  def shutdown
    super
    @logger.info("Shutting down instances..")
    stop_instances(mysqlProvisionedService.all)
  end

  def setup_pool(instance)
    return unless instance
    conn = mysql_connect(instance.ip, false)
    @pool_mutex.synchronize do
      @pools[instance.name] = conn
    end
  end

  def fetch_pool(key)
    return unless key
    @pool_mutex.synchronize do
      @pools[key]
    end
  end
end
