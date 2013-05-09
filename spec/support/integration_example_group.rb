require "uaa/token_coder"
Dir.glob(File.join(File.dirname(__FILE__), '*')).each do |file|
  require file
end

module IntegrationExampleGroup
  include CcngClient

  TMP_DIR = File.expand_path('tmp', SPEC_ROOT)

  def self.included(base)
    base.instance_eval do
      let(:mysql_root_connection) { Sequel.connect("mysql2://root@localhost/mysql") }
      before :each do |example|
        cleanup_mysql_dbs
        (example.example.metadata[:components] || []).each do |component|
          @component_references = {} unless @component_references
          instance = component(component)
          instance.start
          @component_references[instance.class.to_s] = instance
        end
      end
      after :each do |example|
        (example.example.metadata[:components] || []).reverse.each do |component|
          component(component).stop
        end
      end
    end
  end

  def space_guid
    component!(:ccng).space_guid
  end

  def org_guid
    component!(:ccng).org_guid
  end

  def component(name)
    @components ||= {}
    @components[name] ||= self.class.const_get("#{name.capitalize}Runner").new(TMP_DIR)
  end

  def component!(name)
    @components.fetch(name)
  end

  def provision_mysql_instance(name)
    provision_service_instance(name, "mysql", "100")
  end

  def provision_service_instance(name, service_name, plan_name)
    inst_data = ccng_post "/v2/service_instances",
      name: name,
      space_guid: space_guid,
      service_plan_guid: plan_guid(service_name, plan_name)
    inst_data.fetch("metadata").fetch("guid")
  end

  def user_guid
    12345
  end

  def cleanup_mysql_dbs
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

  def plan_guid(service_name, plan_name)
    plans_path = service_response(service_name).fetch("entity").fetch("service_plans_url")
    plan_response(plan_name, plans_path).fetch('metadata').fetch('guid')
  end

  private

  def plan_response(plan_name, plans_path)
    with_retries(30) do
      response = client.get "http://localhost:8181/#{plans_path}", header: { "AUTHORIZATION" => ccng_auth_token }
      res = Yajl::Parser.parse(response.body)
      res.fetch("resources").detect {|p| p.fetch('entity').fetch('name') == plan_name } or
        raise "Could not find plan with name #{plan_name.inspect} in response #{response.body}"
    end
  end

  def service_response(service_name)
    with_retries(30) do
      response = client.get "http://localhost:8181/v2/services", header: { "AUTHORIZATION" => ccng_auth_token }
      res = Yajl::Parser.parse(response.body)
      res.fetch("resources").detect {|service| service.fetch('entity').fetch('label') == service_name } or
        raise "Could not find a service with name #{service_name} in #{response.body}"
    end
  end

  def with_retries(retries, &block)
    begin
      block.call
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
