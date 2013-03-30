require_relative 'component_runner'

class MysqlRunner < ComponentRunner
  def start_redis
    add_pid Process.spawn 'redis-server --port 5454', log_options(:redis)
    wait_for_tcp_ready("Redis", 5454)
  end

  def start
    start_redis
    Dir.chdir("#{SPEC_ROOT}/../ng/mysql") do
      Bundler.with_clean_env do
        sh "bundle install >> #{tmp_dir}/log/bundle.out"
        add_pid Process.spawn(
          {"AUTHORIZATION_TOKEN" => ccng_auth_token},
          "bundle exec bin/mysql_gateway -c #{asset('mysql_gateway.yml')}",
          log_options(:mysql_gateway)
        )
        wait_for_http_ready('Mysql Gateway', 8181)
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
