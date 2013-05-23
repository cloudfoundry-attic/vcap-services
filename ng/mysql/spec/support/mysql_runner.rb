class MysqlRunner < ComponentRunner
  def start_redis
    add_pid Process.spawn "redis-server #{asset "redis.conf"}", log_options(:redis)
    wait_for_tcp_ready("Redis", 5454)
  end

  def start(opts=nil)
    cleanup_mysql_dbs
    start_redis
    Dir.chdir(File.expand_path("../..", File.dirname(__FILE__))) do
      Bundler.with_clean_env do
        sh "bundle install >> #{tmp_dir}/log/bundle.out"
        config = MysqlConfig.new(self, opts)
        add_pid Process.spawn(
          {"AUTHORIZATION_TOKEN" => ccng_auth_token},
          "bundle exec bin/mysql_gateway -c #{config.gateway_file_location}",
          log_options(:mysql_gateway)
        )
        wait_for_tcp_ready('Mysql Gateway', config.gateway_config_hash.fetch('port'))

        begin
          service_name = config.gateway_config_hash["service"]["name"]
          service_provider = config.gateway_config_hash["service"]["provider"] || "core"
          create_service_auth_token(
            service_name,
            'mysql-token',
            service_provider
          )
        rescue CcngClient::UnsuccessfulResponse
          # Failed to add auth token, likely due to duplicate
        end

        add_pid Process.spawn(
          "bundle exec bin/mysql_node -c #{config.node_file_location}",
          log_options(:mysql_node)
        )
        sleep 5
      end
    end
  end

  def mysql_root_connection
    Sequel.connect("mysql2://root@localhost/mysql")
  end

  def cleanup_mysql_dbs
    return if @already_cleaned_up
    @already_cleaned_up = true
    mysql_root_connection["SHOW DATABASES"].each do |row|
      dbname = row[:Database]
      if dbname.match(/^d[0-9a-f]{32}$/) || dbname == "mgmt"
        mysql_root_connection.run "DROP DATABASE #{dbname}"
      end
    end
    mysql_root_connection.run "DELETE FROM mysql.user WHERE host='%' OR host='localhost' and user LIKE 'u%'"
    mysql_root_connection.run "DELETE FROM mysql.db WHERE host='%' OR host='localhost' and user LIKE 'u%' AND db LIKE 'd%'"
    mysql_root_connection.run "CREATE DATABASE mgmt"
  end

  class MysqlConfig
    attr_reader :runner

    def initialize(runner, opts)
      @runner = runner
      write_custom_config(opts)
    end

    def gateway_file_location
      "#{runner.tmp_dir}/config/mysql_gateway.yml"
    end

    def node_file_location
      "#{runner.tmp_dir}/config/mysql_node.yml"
    end

    def gateway_config_hash
      YAML.load_file(gateway_file_location)
    end

    def node_config_hash
      YAML.load_file(node_file_location)
    end

    private

    def base_gateway_config_hash
      YAML.load_file(runner.asset("mysql_gateway.yml"))
    end

    def base_node_config_hash
      YAML.load_file(runner.asset("mysql_node.yml"))
    end


    def write_custom_config(opts)
      FileUtils.mkdir_p("#{runner.tmp_dir}/config")
      gateway_config_hash = base_gateway_config_hash
      node_config_hash = base_node_config_hash
      if opts
        gateway_config_hash['service']['name'] = opts[:service_name] if opts.has_key?(:service_name)
        gateway_config_hash['service']['provider'] = opts[:service_provider] if opts.has_key?(:service_provider)
        gateway_config_hash['service']['blurb'] = opts[:service_blurb] if opts.has_key?(:service_blurb)
        if opts.has_key?(:plan_name)
          gateway_config_hash['service']['plans'] = {opts.fetch(:plan_name) => base_gateway_config_hash.fetch('service').fetch('plans').values.first}
          node_config_hash['plan'] = opts.fetch(:plan_name)
        end
        # ensure that the gateway has a key for the service
        gateway_config_hash['service_auth_tokens'] = {
          "#{gateway_config_hash['service']['name']}_#{gateway_config_hash['service']['provider'] || 'core'}" => 'mysql-token'
        }
      end

      File.write(gateway_file_location, YAML.dump(gateway_config_hash))
      File.write(node_file_location, YAML.dump(node_config_hash))
    end
  end
end
