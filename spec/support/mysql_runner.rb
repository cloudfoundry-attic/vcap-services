require_relative 'component_runner'

class MysqlRunner < ComponentRunner
  def start_redis
    add_pid Process.spawn "redis-server #{asset "redis.conf"}", log_options(:redis)
    wait_for_tcp_ready("Redis", 5454)
  end

  def start(opts=nil)
    cleanup_mysql_dbs
    start_redis
    Dir.chdir("#{SPEC_ROOT}/../ng/mysql") do
      Bundler.with_clean_env do
        sh "bundle install >> #{tmp_dir}/log/bundle.out"
        config = MysqlConfig.new(self, opts)
        add_pid Process.spawn(
          {"AUTHORIZATION_TOKEN" => ccng_auth_token},
          "bundle exec bin/mysql_gateway -c #{config.file_location}",
          log_options(:mysql_gateway)
        )
        wait_for_tcp_ready('Mysql Gateway', config.config_hash.fetch('port'))

        begin
          create_service_auth_token(
            config.config_hash["service"]["name"],
            'mysql-token',
            config.config_hash["service"]["provider"] || "core"
          )
        rescue CcngClient::UnsuccessfulResponse
          # Failed to add auth token, likely due to duplicate
        end

        add_pid Process.spawn(
          "bundle exec bin/mysql_node -c #{asset 'mysql_node.yml'}",
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

    def file_location
      "#{runner.tmp_dir}/config/mysql_gateway.yml"
    end

    def config_hash
      YAML.load_file(file_location)
    end

    private

    def base_config_hash
      YAML.load_file(runner.asset("mysql_gateway.yml"))
    end

    def write_custom_config(opts)
      FileUtils.mkdir_p("#{runner.tmp_dir}/config")
      config_hash = base_config_hash
      if opts
        config_hash['service']['name']     = opts[:service_name]     if opts.has_key?(:service_name)
        config_hash['service']['provider'] = opts[:service_provider] if opts.has_key?(:service_provider)
        config_hash['service']['blurb']    = opts[:service_blurb]    if opts.has_key?(:service_blurb)
        if opts.has_key?(:plan_name)
          config_hash['plans'] = { opts.fetch(:plan_name) => base_config_hash.fetch('service').fetch('plans').values.first }
        end
      end

      File.write(file_location, YAML.dump(config_hash))
    end
  end
end
