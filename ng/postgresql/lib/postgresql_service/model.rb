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
          property :version,    String,  :required => true
          has n, :bindusers

          def prepare
            nil
          end

          def run
            yield self if block_given?
            save
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

        class Wardenprovisionedservice < VCAP::Services::Base::Warden::Service
          include DataMapper::Resource
          include VCAP::Services::Postgresql::Util

          property :name,             String,   :key => true
          # property plan is deprecated. The instances in one node have same plan.
          property :plan,             Integer,  :required => true
          property :quota_exceeded,   Boolean,  :default => false
          property :port,             Integer,   :unique => true
          property :container,        String
          property :ip,               String
          property :default_username, String
          property :default_password, String
          property :version,          String,  :required => true
          has n, :wardenbindusers

          class << self
            attr_reader :max_db_size
            def init(args)
              super(args)
              @@postgresql_config  = args[:postgresql]
              @@xlog_enforce_tolerance  = args[:xlog_enforce_tolerance] || 5
            end
          end

          define_im_properties :xlog_tolerant_times

          def xlog_tolerant?
            !xlog_tolerant_times || xlog_tolerant_times <= @@xlog_enforce_tolerance
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

          def service_port
            case version
            when "9.2"
              5434
            when "9.1"
              5433
            else
              5432
            end
          end

          def start_options
            options = super
            options[:pre_start_script] = {:script => File.join(script_dir, "pre_service_start.sh"), :use_root => true}
            options[:start_script] = {
              :script => "#{service_script} start #{base_dir} #{log_dir} #{common_dir} #{bin_dir} #{service_port} #{@@postgresql_config[version]["pass"]} #{VCAP.local_ip}",
              :use_spawn => true
            }
            options[:service_port] = service_port
            options[:post_start_script] = {:script => "#{File.join(script_dir, "post_service_start.sh")} #{base_dir}", :use_root => true}
            options
          end

          def stop_options
            options = super
            options[:stop_script] = {
              :script => "#{service_script} stop #{base_dir} #{log_dir} #{common_dir} #{bin_dir}",
            }
            options
          end

          def status_options
            options = super
            options[:status_script] = {
              :script => "#{service_script} status #{base_dir} #{log_dir} #{common_dir} #{bin_dir}"
            }
            options
          end

          def finish_start?
            user, pass, database = %w[user pass database].map{ |ele| @@postgresql_config[version][ele] }
            conn = postgresql_connect(ip, user, pass, service_port, database, :quick => true)
            !conn.nil?
          rescue => e
            false
          ensure
            conn.close if conn
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

        def self.pgProvisionedServiceClass(use_warden)
          if use_warden
            VCAP::Services::Postgresql::Node::Wardenprovisionedservice
          else
            VCAP::Services::Postgresql::Node::Provisionedservice
          end
        end

        def self.pgBindUserClass(use_warden)
          if use_warden
            VCAP::Services::Postgresql::Node::WardenBinduser
          else
            VCAP::Services::Postgresql::Node::Binduser
          end
        end

      end
    end
  end
end
