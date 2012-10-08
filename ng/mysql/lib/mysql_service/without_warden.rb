module VCAP
  module Services
    module Mysql
      module WithoutWarden
      end
    end
  end
end

module VCAP::Services::Mysql::WithoutWarden
  def mysqlProvisionedService
    VCAP::Services::Mysql::Node::ProvisionedService
  end

  def pre_send_announcement_internal
    @capacity_lock.synchronize do
      mysqlProvisionedService.all.each do |provisionedservice|
        @capacity -= capacity_unit
      end
    end
  end

  def handle_provision_exception(provisioned_service)
    delete_database(provisioned_service) if provisioned_service
  end

  def help_unprovision(provisioned_service)
    if not provisioned_service.destroy
      @logger.error("Could not delete service: #{provisioned_service.errors.inspect}")
      raise MysqlError.new(MysqError::MYSQL_LOCAL_DB_ERROR)
    end
    # the order is important, restore quota only when record is deleted from local db.
  end

  def fetch_pool(instance)
    @pool
  end

  def get_port(provisioned_service)
    @mysql_config["port"]
  end

  def each_pool
    yield @pool, nil
  end

  #override new_port to make it do nothing
  def new_port(port=nil)
  end

  def method_missing(method_name, *args, &block)
    no_ops = [:init_internal, :setup_pool]
    super unless no_ops.include?(method_name)
  end
end
