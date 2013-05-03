require "spec_helper"

describe "Shared multi-tenant MySQL", components: [:collector, :ccng, :mysql] do
  let(:create_app_request) do
    {
      "space_guid" => space_guid,
      "name" => "mysql_binding_test",
      "instances" => 1,
      "memory" => 256
    }
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
    remaining_dbs = []

    reaction_blk = lambda do |data|
      metric = parse(data)
      if metric[:name] == "services.plans.score" &&
        metric[:tags][:job] && metric[:tags][:job] == "MyaaS-Provisioner" &&
        metric[:tags][:plan] && metric[:tags][:plan] == "100"

        remaining_dbs << Integer(metric[:value])
      end
    end
    @components.fetch(:collector).reaction_blk = reaction_blk
    provision_mysql_instance("new_db")

    final_db_count(remaining_dbs).should == initial_db_count(remaining_dbs) - 1
  end

  def initial_db_count(remaining_dbs)
    remaining_dbs.first
  end

  def final_db_count(remaining_dbs)
    collector_poll_frequency_in_seconds = 10
    print "Waiting up to 60s for our VARZ to report"
    6.times do
      if remaining_dbs.first == remaining_dbs.last
        print "."
        sleep collector_poll_frequency_in_seconds
      else
        return remaining_dbs.last
      end
    end
    remaining_dbs.last
  end

  def bind_service
    instance_guid = provision_mysql_instance("yoursql")
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
