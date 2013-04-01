require "uaa/token_coder"
Dir.glob(File.join(File.dirname(__FILE__), '*')).each do |file|
  require file
end

module IntegrationExampleGroup
  include CcngClient

  TMP_DIR = File.expand_path('../tmp', SPEC_ROOT)

  def self.included(base)
    base.instance_eval do
      before :each do |example|
        (example.example.metadata[:components] || []).each do |component|
          component(component).start
        end
      end
      after :each do |example|
        (example.example.metadata[:components] || []).each do |component|
          component(component).stop
        end
      end
    end
  end

  def space_guid
    component!(:ccng).space_guid
  end

  def org_guid
    component!(:ccng).space_guid
  end

  def component(name)
    @components ||= {}
    @components[name] ||= self.class.const_get("#{name.capitalize}Runner").new(TMP_DIR)
  end

  def component!(name)
    @components.fetch(name)
  end

  def provision_mysql_instance(name)
    inst_data = ccng_post "/v2/service_instances",
      {name: name, space_guid: space_guid, service_plan_guid: plan_guid('mysql', '100')}
    inst_data.fetch("metadata").fetch("guid")
  end

  def user_guid
    12345
  end


  def create_service_auth_token(label, service_token)
    ccng_post("/v2/service_auth_tokens",
              {label: label, provider:'core', token: service_token}
             )
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
