require_relative 'component_runner'

class CcngRunner < ComponentRunner
  attr_reader :org_guid, :space_guid

  def stop
    kill_listening_on_port(8181)
  end

  def checkout_ccng
    ENV["CC_BRANCH"] ||= "origin/master"
    Dir.chdir tmp_dir do
      FileUtils.mkdir_p "log"
      sh "git clone --recursive git://github.com/cloudfoundry/cloud_controller_ng.git" unless Dir.exist?("cloud_controller_ng")
      Dir.chdir "cloud_controller_ng" do
        if ENV['NO_CHECKOUT'].nil? || ENV['NO_CHECKOUT'].empty?
          unless `git status -s`.empty?
            raise 'There are outstanding changes in cloud controller. Need to set NO_CHECKOUT env'
          end
          sh "git fetch && git reset --hard #{ENV['CC_BRANCH'] || "origin/master"} && git submodule update --init"
        end

        Bundler.with_clean_env do
          sh "bundle install >> #{tmp_dir}/log/bundle.out"
        end
      end
      $checked_out = true
    end
  end

  def start
    checkout_ccng unless $checked_out
    FileUtils.mkdir_p("#{tmp_dir}/config")
    database_file = "/tmp/cloud_controller.db"
    FileUtils.rm_f(database_file)
    Dir.chdir "#{tmp_dir}/cloud_controller_ng" do
      File.open("#{tmp_dir}/config/cloud_controller.yml", "w") do |f|
        f.write YAML.dump(YAML.load_file("config/cloud_controller.yml").merge({
          "logging" => {
            "file" => "#{tmp_dir}/log/cloud_controller.log",
            "level" => "info",
            "db" => {
              "database" => "sqlite://#{database_file}",
              "max_connections" => 32,
              "pool_timeout" => 10,
            }
          }
        }))
      end
      Bundler.with_clean_env do
        FileUtils.rm_f "/tmp/cloud_controller.db"
        sh "bundle exec rake db:migrate >> /dev/null"
        sh %q{sqlite3 /tmp/cloud_controller.db 'INSERT INTO quota_definitions(guid, created_at, name, non_basic_services_allowed, total_services, memory_limit) VALUES("test_quota", "2010-01-01", "free", 1, 100, 1024)'}
        add_pid Process.spawn "bundle exec ./bin/cloud_controller --config #{tmp_dir}/config/cloud_controller.yml", log_options(:cloud_controller)
      end
    end
    wait_for_http_ready("CCNG", 8181)

    setup_ccng_orgs_and_spaces
  end

  def setup_ccng_orgs_and_spaces
    user_guid = '12345'
    @org_guid = ccng_post(
      "/v2/organizations",
      {name: 'test_org', user_guids: [user_guid]}
    ).fetch("metadata").fetch("guid")

    @space_guid = ccng_post(
      "/v2/spaces",
      {name: 'test_space', organization_guid: @org_guid}
    ).fetch("metadata").fetch("guid")

    ccng_put("/v2/spaces/#{@space_guid}/developers/#{user_guid}", {})
  end
end
