module VCAP
  module Services
    module Mysql
      module Warden

        def mysqlProvisionedService
          VCAP::Services::Mysql::Node::WardenProvisionedService
        end

        def init_internal(options)
          @service_start_timeout = @options[:service_start_timeout] || 3
          init_ports(options[:port_range])
        end

        def pre_send_announcement_internal
          @capacity_lock.synchronize do
            start_instances(mysqlProvisionedService.all)
          end

          mysqlProvisionedService.all.each do |instance|
            @pools[instance.port] = mysql_connect(instance.ip, false)
          end
        end

        def prepare_environment(provisioned_service)
          @pools[provisioned_service.port] = mysql_connect(provisioned_service.ip, false)
        end

        def handle_provision_exception(provisioned_service)
          return unless provisioned_service
          port = provisioned_service.port
          provisioned_service.delete
          free_port(port) if port
        end

        def help_unprovision(provisioned_service)
          port = provisioned_service.port
          @pools[port].shutdown
          @pools.delete(port)
          raise "Could not cleanup instance #{provisioned_service.name}" unless provisioned_service.delete
          free_port(port)
        end

        def get_port(provisioned_service)
          provisioned_service.port
        end

        def is_service_started(instance)
          get_status(instance) == "ok"
        end

        def shutdown
          super
          @logger.info("Shutting down instances..")
          mysqlProvisionedService.all.each do |instance|
            @logger.debug("Try to terminate mysql container: #{instance.name}")
            instance.stop if instance.running?
          end
        end
      end
    end
  end
end
