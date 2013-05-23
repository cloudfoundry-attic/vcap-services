require "spec_helper"
require "sequel"

describe "Shared multi-tenant MySQL", components: [:collector, :nats, :ccng, :mysql] do
  let(:collector_poll_frequency_in_seconds) { 1 }
  let(:create_app_request) do
    {
      "space_guid" => space_guid,
      "name" => "mysql_binding_test",
      "instances" => 1,
      "memory" => 256
    }
  end

  before do
    login_to_ccng_as('12345', 'sre@vmware.com')
  end

  it "registers an offering with extra and unique id" do
    mysql_service = ccng_get("/v2/services").fetch("resources").first.fetch("entity")
    mysql_service.fetch("extra").should include("http://example.com/pretty_pikature.gif")
    mysql_service.fetch("unique_id").should include("mysql_service_unique_id")
    ccng_get("/v2/service_plans").fetch("resources").first.fetch("entity").fetch("unique_id").should == 'rds_mysql_plan_unique_id'
  end

  it "can provision a service instance" do
    ccng_get("/v2/spaces/#{space_guid}/service_instances").fetch("total_results").should == 0
    expect {
      provision_mysql_instance("yoursql")
      provision_mysql_instance("oursql")
    }.to change { mysql_root_connection["SHOW DATABASES"].to_a.size }.by(2)
    ccng_get("/v2/spaces/#{space_guid}/service_instances").fetch("total_results").should == 2
  end

  it "can bind a service instance" do
    conn_string = get_creds(bind_service)
    Sequel.connect(conn_string) do |conn|
      expect {
        conn.run("CREATE TABLE ponies(hay INTEGER)")
      }.to change { conn["SHOW TABLES"].to_a.length }.from(0).to(1)
    end
  end

  it "can unbind a service instance" do
    binding_response = bind_service
    ccng_delete("/v2/service_bindings/#{binding_response.fetch("metadata").fetch("guid")}")
    conn_string = get_creds(binding_response)
    expect {
      Sequel.connect(conn_string) do |conn|
        conn.run("CREATE TABLE ponies(hay INTEGER)")
      end
    }.to raise_error(Sequel::DatabaseConnectionError, /Access denied/)
  end

  it "can delete a service instance" do
    service_instance_guid = provision_mysql_instance("oursql")
    ccng_delete("/v2/service_instances/#{service_instance_guid}")

    ccng_get("/v2/spaces/#{space_guid}/service_instances").fetch("total_results").should == 0
  end

  context "with innodb_file_per_table set in mysqld's config" do
    it "prevents further writes after quota exceeded then allows writes after quota obeyed" do
      conn_string = get_creds(bind_service)
      Sequel.connect(conn_string) do |conn|
        conn.run("CREATE TABLE table1(stuff char(200))")
        conn.run("CREATE TABLE table2(stuff char(200))") # made 2 tables and clear 1 completely to get mysql to reclaim space more consistently
        conn.run("INSERT INTO table1 VALUES('I am the walrus')")
        conn.run("INSERT INTO table2 VALUES('I am the walrus')")
        11.times do
          conn.run "INSERT INTO table1 SELECT * FROM table1"
          conn.run "INSERT INTO table2 SELECT * FROM table2"
        end
      end

      expect_statement_denied!(conn_string, "INSERT INTO table1 VALUES ('should_fail')")
      expect_statement_denied!(conn_string, "UPDATE table1 SET stuff='ponies'")

      expect_statement_allowed!(conn_string, 'select count(*) from table1')
      expect_statement_allowed!(conn_string, 'delete from table1')

      expect_statement_allowed!(conn_string, "INSERT INTO table1 VALUES ('should_work')")
      expect_statement_allowed!(conn_string, "UPDATE table1 SET stuff='ponies'")
    end
  end

  it "kills long running transactions" do
    conn_string = get_creds(bind_service)
    max_long_tx_secs = 30
    Sequel.connect(conn_string) do |conn|
      conn.run("CREATE TABLE table1(stuff int);")
      conn.run("BEGIN")
      conn.run("INSERT INTO table1 VALUES (2);")
      print "Waiting up to #{max_long_tx_secs*2}s for our TXn to be killed"
      expect {
        (max_long_tx_secs * 2).times do
          print "."
          conn.run("SELECT SLEEP(1)")
        end
      }.to raise_error Sequel::DatabaseDisconnectError
      puts "Done!"
    end
    # what happens?
  end

  it "advertises number of remaining databases in VARZ" do
    metrics = {}
    reaction_blk = lambda do |data|
      metric = parse(data)
      if metric[:tags][:job] && metric[:tags][:job] == "MyaaS-Provisioner" &&
        metric[:tags][:plan] && metric[:tags][:plan] == "100"

        metrics[metric[:name]] ||= []
        metrics[metric[:name]] << Integer(metric[:value])
      end
    end
    @components.fetch(:collector).reaction_blk = reaction_blk

    initial_db_count(metrics, "services.plans.available_capacity").should == 200
    initial_db_count(metrics, "services.plans.used_capacity").should == 0

    provision_mysql_instance("new_db")

    final_db_count(metrics, "services.plans.available_capacity").should == 199
    final_db_count(metrics, "services.plans.used_capacity").should == 1
  end

  it "properly updates the label and provider of an existing service" do
    old_unique_id = service_response("mysql").fetch("entity").fetch("unique_id")
    ccng_get("/v2/services").fetch("resources").should have(1).item
    provision_service_instance("before", "mysql", "100")
    component!(:mysql).stop

    component!(:mysql).start(service_blurb: 'something totally different')
    extra_json = service_response("mysql").fetch("entity").fetch("extra")
    JSON.parse(extra_json)['listing']['blurb'].should == 'something totally different'
    component!(:mysql).stop

    component!(:mysql).start(
     service_name: 'different-mysql',
     service_provider: 'someoneelse',
     plan_name: 'expensive',
    )
    provision_service_instance("after", "different-mysql", "expensive")
    service_response("different-mysql").fetch("entity").fetch("provider").should == "someoneelse"
    service_response("different-mysql").fetch("entity").fetch("unique_id").should == old_unique_id
    ccng_get("/v2/services").fetch("resources").should have(1).item
  end

  it "gracefully handles gateway restarts" do
    before_guid = provision_mysql_instance("before")
    component!(:mysql).stop
    component!(:mysql).start
    bind_service(before_guid)
  end

  def initial_db_count(metrics, key)
    print "Waiting up to 60s for our VARZ to report"
    60.times do
      if metrics[key] && metrics[key].first
        return metrics[key].first
      else
        print "."
        sleep collector_poll_frequency_in_seconds
      end
    end
    metrics.keys.should include(key)
  ensure
    puts
  end

  def final_db_count(metrics, key)
    print "Waiting up to 60s for our VARZ to report"
    60.times do
      if metrics[key] && metrics[key].first != metrics[key].last
        return metrics[key].last
      else
        print "."
        sleep collector_poll_frequency_in_seconds
      end
    end
    metrics.keys.should include(key)
  ensure
    puts
  end

  def bind_service(instance_guid = provision_mysql_instance("yoursql"))
    app_guid = ccng_post("/v2/apps", create_app_request).fetch("metadata").fetch("guid")

    create_binding_request = {
      app_guid: app_guid, service_instance_guid: instance_guid
    }
    ccng_post("/v2/service_bindings", create_binding_request)
  end

  def get_creds(binding_response)
    creds = binding_response.fetch("entity").fetch("credentials")
    "mysql2://#{creds["user"]}:#{creds["password"]}@#{creds["host"]}:#{creds["port"]}/#{creds["name"]}"
  end

  def parse(data)
    output = data.split(" ", 5)
    tags = output[4].split(" ")
    new_tags = {}
    tags.each do |tag|
      split = tag.split("=")
      new_tags[split[0].to_sym] = split[1]
    end
    {
      verb: output[0],
      name: output[1],
      timestamp: output[2],
      value: output[3],
      tags: new_tags
    }
  end

  def expect_statement_allowed!(conn_string, sql)
    lastex = nil
    100.times do
      begin
        Sequel.connect(conn_string) do |conn|
          sleep 0.1
          conn.run(sql)
        end
        return true
      rescue => e
        lastex = e
        # ignore
      end
    end
    raise "Timed out waiting for #{sql} to be allowed, last exception #{lastex.inspect}"
  end

  def expect_statement_denied!(conn_string, sql)
    expect do
      100.times do
        Sequel.connect(conn_string) do |conn|
          sleep 0.1
          conn.run(sql)
        end
      end
    end.to raise_error(/command denied/)
  end
end
