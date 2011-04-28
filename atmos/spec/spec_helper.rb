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
    :aux => config[:aux]
  }
  options[:logger].level = Logger::FATAL
  options
end

def start_server(opts)
  sp = Provisioner.new(@opts).start()
  opts = opts.merge({:provisioner => sp})
  sg = VCAP::Services::AsynchronousServiceGateway.new(opts)
  Thin::Server.start(opts[:host], opts[:port], sg)
end


def get_raw_config()
  config_file = File.join(File.dirname(__FILE__), "../config/atmos_gateway.yml")
  config = YAML.load_file(config_file)
  config = symbolize_keys(config)
end
