module VCAP
  module Services
    module Mysql
      module NonWarden
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

        def get_port(provisioned_service)
          @mysql_config["port"]
        end

        #override new_port to make it do nothing
        def new_port(port=nil)
        end

        def method_missing(method_name, *args, &block)
          no_ops = [:init_internal, :prepare_environment]
          super unless no_ops.include?(method_name)
        end

      end
    end
  end
end
