require_relative 'component_runner'

class MysqlRunner < ComponentRunner
  def start_redis
    add_pid Process.spawn "redis-server #{asset "redis.conf"}", log_options(:redis)
    wait_for_tcp_ready("Redis", 5454)
  end

  def gateway_config_hash
    YAML.load_file(gateway_config_file)
  end

  def gateway_config_file
    asset('mysql_gateway.yml')
  end

  def start
    start_redis
    Dir.chdir("#{SPEC_ROOT}/../ng/mysql") do
      Bundler.with_clean_env do
        sh "bundle install >> #{tmp_dir}/log/bundle.out"
        add_pid Process.spawn(
          {"AUTHORIZATION_TOKEN" => ccng_auth_token},
          "bundle exec bin/mysql_gateway -c #{gateway_config_file}",
          log_options(:mysql_gateway)
        )
        wait_for_tcp_ready('Mysql Gateway', gateway_config_hash.fetch('port'))
        create_service_auth_token('mysql', 'mysql-token')

        add_pid Process.spawn(
          "bundle exec bin/mysql_node -c #{asset 'mysql_node.yml'}",
          log_options(:mysql_node)
        )
        sleep 5
      end
    end
  end
end
