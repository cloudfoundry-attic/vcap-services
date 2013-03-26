require "uaa/token_coder"

module IntegrationExampleGroup
  TMP_DIR = File.expand_path('../tmp', SPEC_ROOT)

  def self.included(base)
    base.instance_eval do
      attr_reader :space_guid, :org_guid
      before :each do |example|
        (example.example.metadata[:components] || []).each do |component|
          public_send("start_#{component}")
        end
      end
      after :each do |example|
        (example.example.metadata[:components] || []).each do |component|
          public_send("stop_#{component}")
        end
      end
    end
  end

  def start_ccng
    # set up the required directories
    # clone cloud controller
    #   check out correct branch
    #   update submodules
    #
    # start nats
    # start the ccng
    start_nats
    checkout_ccng unless $checked_out
    Dir.chdir "#{TMP_DIR}/cloud_controller_ng" do
      Bundler.with_clean_env do
        FileUtils.rm_f "/tmp/cloud_controller.db"
        sh "bundle exec rake db:migrate"
        sh %q{sqlite3 /tmp/cloud_controller.db 'INSERT INTO quota_definitions(guid, created_at, name, non_basic_services_allowed, total_services, memory_limit) VALUES("test_quota", "2010-01-01", "free", 1, 100, 1024)'}
        @ccng_pid = Process.spawn "bundle exec ./bin/cloud_controller", log_options(:cloud_controller)
      end
    end
    wait_for_http_ready("CCNG", 8181)

    setup_ccng_orgs_and_spaces
  end

  def stop_ccng
    kill_process(@ccng_pid)
    stop_nats
  end


  def start_mysql
    start_redis
    Dir.chdir("#{SPEC_ROOT}/../ng/mysql") do
      Bundler.with_clean_env do
        sh "bundle install >> #{TMP_DIR}/log/bundle.out"
        @mysql_gateway_pid = Process.spawn(
          {"AUTHORIZATION_TOKEN" => ccng_auth_token},
          "bundle exec bin/mysql_gateway -c #{asset('mysql_gateway.yml')}",
          log_options(:mysql_gateway)
        )
        wait_for_http_ready('Mysql Gateway', 8181)
        create_service_auth_token('mysql', 'mysql-token')

        @mysql_node_pid = Process.spawn(
          "bundle exec bin/mysql_node -c #{asset 'mysql_node.yml'}",
          log_options(:mysql_node)
        )
        sleep 5
      end
    end
  end

  def stop_mysql
    stop_redis
    kill_process @mysql_gateway_pid
    kill_process @mysql_node_pid
  end

  def provision_mysql_instance(name)
    inst_data = ccng_post "/v2/service_instances",
      {name: name, space_guid: space_guid, service_plan_guid: plan_guid('mysql', '100')}
    inst_data.fetch("metadata").fetch("guid")
  end

  private

  def make_ccng_request(method, resource_path, body_hash=nil)
    uri = URI.parse("http://127.0.0.1:8181/")
    uri.path = resource_path
    response = client.public_send(method,
                                  uri,
                                  header: { "AUTHORIZATION" => ccng_auth_token },
                                  body: Yajl::Encoder.encode(body_hash)
                                 )
    raise "Unexpected response from #{resource_path}: #{response.inspect}" unless response.ok?
    Yajl::Parser.parse(response.body)
  end

  def client
    HTTPClient.new
  end

  def kill_process(pid)
    Process.kill "TERM", pid if pid
  end

  def start_nats
    unless @nats_pid
      @nats_pid = Process.spawn "bundle exec nats-server", log_options(:nats)
      wait_for_tcp_ready("NATS", 4222)
    end
  end

  def stop_nats
    kill_process @nats_pid
  end

  def start_redis
    unless @redis_pid
      @redis_pid = Process.spawn 'redis-server --port 5454', log_options(:redis)
      wait_for_tcp_ready("Redis", 5454)
    end
  end

  def stop_redis
    kill_process @redis_pid
  end

  def checkout_ccng
    ENV["CC_BRANCH"] ||= "origin/master"
    Dir.chdir TMP_DIR do
      FileUtils.mkdir_p "log"
      sh "git clone --recursive git@github.com:cloudfoundry/cloud_controller_ng.git" unless Dir.exist?("cloud_controller_ng")
      Dir.chdir "cloud_controller_ng" do
        if ENV['NO_CHECKOUT'].nil?
          unless `git status -s`.empty?
            raise 'There are outstanding changes in cloud controller. Need to set NO_CHECKOUT env'
          end
        else
          sh "git fetch && git reset --hard #{ENV['CC_BRANCH']}"
        end

        Bundler.with_clean_env do
          sh "bundle install >> #{TMP_DIR}/log/bundle.out"
        end
      end
      $checked_out = true
    end
  end

  def sh(cmd)
    raise "Unable to run #{cmd} in #{Dir.pwd}" unless system(cmd)
  end

  def log_options(name)
    {:out => "#{TMP_DIR}/log/#{name}.out", :err => "#{TMP_DIR}/log/#{name}.err"}
  end

  def wait_for_http_ready(label, port)
    print "Waiting for #{label}..."
    retries = 30
    begin
      response = client.get("http://localhost:#{port}/info")
      raise "Failed to connect, status: #{response.status}" unless response.ok?
      puts "ready!"
    rescue
      print "."
      sleep 0.3
      retries -= 1
      if retries > 0
        retry
      else
        raise
      end
    end
  end

  def wait_for_tcp_ready(label, port)
    print "Waiting for #{label}..."
    retries = 30
    begin
      sock = TCPSocket.new("localhost", port)
      sock.close
      puts "ready!"
    rescue
      print "."
      sleep 0.3
      retries -= 1
      if retries > 0
        retry
      else
        raise
      end
    end
  end

  def user_guid
    12345
  end

  def ccng_auth_token
    token_coder = CF::UAA::TokenCoder.new(:audience_ids => "cloud_controller",
                                          :skey => "tokensecret", :pkey => nil)

    user_token = token_coder.encode(
      :user_id => user_guid,
      :client_id => "vmc",
      :email => "sre@vmware.com",
      :scope => %w[cloud_controller.admin]
    )

    "bearer #{user_token}"
  end

  def ccng_post(resource_path, body_hash)
    make_ccng_request(:post, resource_path, body_hash)
  end

  def ccng_put(resource_path, body_hash)
    make_ccng_request(:put, resource_path, body_hash)
  end

  def ccng_get(resource_path)
    make_ccng_request(:get, resource_path)
  end

  def create_service_auth_token(label, service_token)
    ccng_post("/v2/service_auth_tokens",
              {label: label, provider:'core', token: service_token}
             )
  end

  def setup_ccng_orgs_and_spaces
    @org_guid = ccng_post(
      "/v2/organizations",
      {name: 'test_org', user_guids: [user_guid.to_s]}
    ).fetch("metadata").fetch("guid")

    @space_guid = ccng_post(
      "/v2/spaces",
      {name: 'test_space', organization_guid: @org_guid}
    ).fetch("metadata").fetch("guid")

    ccng_put("/v2/spaces/#{@space_guid}/developers/#{user_guid}", {})
  end

  def asset(file_name)
    File.expand_path(File.join(SPEC_ROOT, 'assets', file_name))
  end

  def plan_guid(service_name, plan_name)  # ignored for now, hoping the first one is correct
    retries = 30
    begin
      response = client.get "http://localhost:8181/v2/services",
        header: { "AUTHORIZATION" => ccng_auth_token }
      res = Yajl::Parser.parse(response.body)
      raise "Could not find any resources: #{response.body}" if res.fetch("resources").empty?
      plans_path = res.fetch("resources")[0].fetch("entity").fetch("service_plans_url")
      response = client.get "http://localhost:8181/#{plans_path}",
        header: { "AUTHORIZATION" => ccng_auth_token }
      res = Yajl::Parser.parse(response.body)
      res.fetch("resources")[0].fetch('metadata').fetch('guid')
    rescue
      retries -= 1
      sleep 0.3
      if retries > 0
        retry
      else
        raise
      end
    end
  end
end
