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
            self.destroy!
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

        class Wardenprovisionedservice
          @@new_iptables_lock = Mutex.new
          @@prepare_filesystem_lock = Mutex.new
          include DataMapper::Resource
          include VCAP::Services::Base::Utils
          include VCAP::Services::Base::Warden

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
              raise "Parameter :base_dir missing" unless args[:base_dir]
              raise "Parameter :log_dir missing" unless args[:log_dir]
              raise "Parameter :image_dir missing" unless args[:image_dir]
              @@options = args
              @base_dir            = args[:base_dir]
              @log_dir             = args[:log_dir]
              @image_dir           = args[:image_dir]
              @logger              = args[:logger]
              @max_db_size         = ((args[:max_db_size] + args[:db_size_overhead]) * 1024 * 1024).round
              @max_disk            = (args[:disk_overhead] + args [:max_db_size] + args[:db_size_overhead]).ceil
              @quota               = args[:filesystem_quota] || false

              FileUtils.mkdir_p(base_dir)
              FileUtils.mkdir_p(log_dir)
              FileUtils.mkdir_p(image_dir)
            end
          end

          def default_user
            ret = nil
            if self.default_username
              ret = {:user => self.default_username, :password => self.default_password}
            end
            ret
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

          alias_method :iptable_ori, :iptable
          def iptable(add, src_port, dest_ip, dest_port)
            rule = [ "--protocol tcp",
                 "--dport #{src_port}",
                 "--jump DNAT",
                 "--to-destination #{dest_ip}:#{dest_port}" ]

            if add
              cmd1 = "iptables -t nat -A PREROUTING #{rule.join(" ")}"
              cmd2 = "iptables -t nat -A OUTPUT #{rule.join(" ")}"
            else
              cmd1 = "iptables -t nat -D PREROUTING #{rule.join(" ")}"
              cmd2 = "iptables -t nat -D OUTPUT #{rule.join(" ")}"
            end

            # iptables exit code:
            # The exit code is 0 for correct functioning.
            # Errors which appear to be caused by invalid or abused command line parameters cause an exit code of 2,
            # and other errors cause an exit code of 1.
            #
            # We add a thread lock here, since iptables may return resource unavailable temporary in multi-threads
            # iptables command issued.
            @@new_iptables_lock.synchronize do
              ret = self.class.sh(cmd1, :raise => false)
              logger.warn("cmd \"#{cmd1}\" invalid") if ret == 2
              ret = self.class.sh(cmd2, :raise => false)
              logger.warn("cmd \"#{cmd2}\" invalid") if ret == 2
            end
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
