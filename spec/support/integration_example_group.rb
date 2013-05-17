require "uaa/token_coder"
require 'fileutils'
require 'active_support/core_ext'
Dir.glob(File.join(File.dirname(__FILE__), '*')).each do |file|
  require file
end

module IntegrationExampleGroup
  include CcngClient

  def self.tmp_dir
    @@tmp_dir ||= File.expand_path('tmp', SPEC_ROOT)
  end

  def self.tmp_dir=(new_value)
    @@tmp_dir = new_value
  end

  def tmp_dir
    IntegrationExampleGroup.tmp_dir
  end

  def self.included(base)
    base.instance_eval do
      metadata[:type] = :integration
      let(:mysql_root_connection) { component!(:mysql).mysql_root_connection }
      before :each do |example|
        (example.example.metadata[:components] || []).each do |component|
          instance = component(component)
          instance.start
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
    FileUtils.mkdir_p(tmp_dir)
    @components[name] ||= self.class.const_get("#{name.to_s.camelize}Runner").new(tmp_dir)
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
