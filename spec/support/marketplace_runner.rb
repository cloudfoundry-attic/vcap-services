require_relative 'component_runner'

class MarketplaceRunner < ComponentRunner
  def gateway_config_hash
    YAML.load_file(gateway_config_file)
  end

  def gateway_config_file
    asset('marketplace_gateway.yml')
  end

  def start
    start_app_direct_server

    Dir.chdir("#{SPEC_ROOT}/../marketplace") do
      Bundler.with_clean_env do
        sh "bundle install >> #{tmp_dir}/log/bundle.out"
        add_pid Process.spawn(
          {"AUTHORIZATION_TOKEN" => ccng_auth_token},
          "bundle exec bin/marketplace_gateway -c #{gateway_config_file}",
          log_options(:marketplace)
        )
        wait_for_tcp_ready('Marketplace Gateway', gateway_config_hash.fetch('port'))
        create_service_auth_token('marketplace', 'marketplace-token')
      end
    end
  end

  def start_app_direct_server
    Bundler.with_clean_env do
      add_pid Process.spawn(
        %q{bundle exec ruby -I. -r support/fake_app_direct_server.rb -e 'FakeAppDirectRunner.start'},
        log_options(:fake_app_direct)
      )
      wait_for_tcp_ready('Fake AppDirect server', 9999)
    end
  end
end
