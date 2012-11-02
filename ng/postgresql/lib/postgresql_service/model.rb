module VCAP
  module Services
    module Postgresql

      class Node
        class Provisionedservice
          include DataMapper::Resource
          property :name,       String,   :key => true
          # property plan is deprecated. The instances in one node have same plan.
          property :plan,       Integer, :required => true
          property :quota_exceeded,  Boolean, :default => false
          has n, :bindusers

          def prepare
            nil
          end

          def run
            nil
          end

          def delete
            self.destroy! if self.saved?
          end

          def pgbindusers
            self.bindusers
          end

          def default_user
            self.bindusers(:default_user => true)[0]
          end

        end

        class Binduser
          include DataMapper::Resource
          property :user,       String,   :key => true
          property :sys_user,    String,    :required => true
          property :password,   String,   :required => true
          property :sys_password,    String,    :required => true
          property :default_user,  Boolean, :default => false
          belongs_to :provisionedservice
        end

        class Wardenprovisionedservice < VCAP::Services::Base::WardenService
          include DataMapper::Resource

          property :name,             String,   :key => true
          # property plan is deprecated. The instances in one node have same plan.
          property :plan,             Integer,  :required => true
          property :quota_exceeded,   Boolean,  :default => false
          property :port,             Integer,   :unique => true
          property :container,        String
          property :ip,               String
          property :default_username, String
          property :default_password, String
          has n, :wardenbindusers

          class << self
            attr_reader :max_db_size
            def init(args)
              super(args)
              @max_db_size         = ((args[:max_db_size] + args[:db_size_overhead]) * 1024 * 1024).round
              @max_disk            = (args[:disk_overhead] + args [:max_db_size] + args[:db_size_overhead]).ceil
            end
          end

          def default_user
            ret = nil
            if self.default_username
              ret = {:user => self.default_username, :password => self.default_password}
            end
            ret
          end

          alias_method :delete_ori, :delete
          def delete
            # stop container
            begin
              stop if running?
            rescue
              # Catch the exception and record error log here to guarantee the following cleanup work is done.
              logger.error("Fail to stop container when deleting service #{self[:name]}")
            end
            # delete log and service directory
            if self.class.quota
              self.class.sh "rm -rf #{image_file}"
            end
            self.class.sh "rm -rf #{base_dir}"
            self.class.sh "rm -rf #{log_dir}"
            util_dirs.each do |util_dir|
              self.class.sh "rm -rf  #{util_dir}"
            end
            Process.detach(pid) if pid
            # delete the record when it's saved
            destroy! if saved?
          end

          def prepare
            raise "Missing name in WardenProvisionedservice instance" unless self.name
            raise "Missing port in WardenProvisionedservice instance" unless self.port
            unless self.pgbindusers.all(:default_user => true)[0]
              raise "Missing default user in WardenProvisionedservice instance"
            end
            default_user = self.pgbindusers.all(:default_user => true)[0]
            self.default_username = default_user[:user]
            self.default_password = default_user[:password]
            logger.debug("Prepare filesytem for instance #{self.name}")
            exception = nil
            begin
              prepare_filesystem(self.class.max_disk)
            rescue => e
              logger.error("Failed to prepare file system for #{e}:#{e.backtrace.join('|')}")
              exception = e
            end
            raise exception if exception
          end

          alias_method :loop_setdown_ori, :loop_setdown
          def loop_setdown
            exception = nil
            begin
              loop_setdown_ori
            rescue => e
              logger.error("Failed to setdown loop file for #{e}: #{e.backtrace.join('|')}")
              exception = e
            end
            raise exception if exception
          end

          def pgbindusers
            wardenbindusers
          end

          def service_port
            "5432"
          end

          def service_script
            "postgresql_ctl"
          end

        end

        class Wardenbinduser
          include DataMapper::Resource
          property :user,       String,   :key => true
          property :sys_user,    String,    :required => true
          property :password,   String,   :required => true
          property :sys_password,    String,    :required => true
          property :default_user,  Boolean, :default => false
          belongs_to :wardenprovisionedservice
        end

        def self.setup_datamapper(sym, orm_db, auto_upgrade=true)
          DataMapper.setup(sym, orm_db)
          DataMapper::auto_upgrade! if auto_upgrade
        end

      end
    end
  end
end
