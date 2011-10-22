# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')
$LOAD_PATH.unshift(File.expand_path("../../../", __FILE__))

require "rubygems"
require "rspec"

HTTP_PORT = 9865

def symbolize_keys(hash)
  if hash.is_a? Hash
    new_hash = {}
    hash.each do |k, v|
      new_hash[k.to_sym] = symbolize_keys(v)
    end
    new_hash
  else
    hash
  end
end

def parse_property(hash, key, type, options = {})
  obj = hash[key]
  if obj.nil?
    raise "Missing required option: #{key}" unless options[:optional]
    nil
  elsif type == Range
    raise "Invalid Range object: #{obj}" unless obj.kind_of?(Hash)
    first, last = obj["first"], obj["last"]
    raise "Invalid Range object: #{obj}" unless first.kind_of?(Integer) and last.kind_of?(Integer)
    Range.new(first, last)
  else
    raise "Invalid #{type} object: #{obj}" unless obj.kind_of?(type)
    obj
  end
end

# Atmos configuration.  The atmos service we test against is a shared,
# off-box instace, so we don't want to bake the credentials into the
# config file.  To avoid having to update the config file manually
# prior to testing, we load the atmos config from the environment.
def check_provisioner_config
  vars = ["VCAP_ATMOS_HOST", "VCAP_ATMOS_TENANT", "VCAP_ATMOS_TENANT_ADMIN", "VCAP_ATMOS_TENANT_PASSWD"]
  vars.each do |e|
    if ENV[e].nil?
      pending "Disabling atmos tests. Set the following environment variables to run them: #{vars.inspect}"
      return false
    end
  end
  true
end

def get_provisioner_config()
  config_file = File.join(File.dirname(__FILE__), "../config/atmos_gateway.yml")
  config = YAML.load_file(config_file)
  config = symbolize_keys(config)
  options = {
    :logger => Logger.new(parse_property(config, "log_file", String, :optional => true) || STDOUT, "daily"),
    # Following options are for Provisioner
    :version => config[:service][:version],
    :local_ip => 'localhost',
    # Following options are for AsynchronousServiceGateway
    :service => config[:service],
    :token => config[:token],
    :cloud_controller => config[:cloud_controller],
    # Following options are for Thin
    :host => 'localhost',
    :port => HTTP_PORT,
    :additional_options => {:atmos => {
      :host => ENV['VCAP_ATMOS_HOST'],
      :port => ENV['VCAP_ATMOS_PORT'] || "443",
      :tenant => ENV['VCAP_ATMOS_TENANT'],
      :tenantadmin => ENV['VCAP_ATMOS_TENANT_ADMIN'],
      :tenantpasswd => ENV['VCAP_ATMOS_TENANT_PASSWD']
    }}
  }

  options[:logger].level = Logger::DEBUG
  options
end

def start_server(opts)
  sp = Provisioner.new(@opts).start()
  opts = opts.merge({:provisioner => sp})
  sg = VCAP::Services::AsynchronousServiceGateway.new(opts)
  Thin::Server.start(opts[:host], opts[:port], sg)
end

