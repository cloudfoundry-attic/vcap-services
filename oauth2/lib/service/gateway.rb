require 'vcap_services_base'

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', 'lib')
require 'service/provisioner'

class CF::UAA::OAuth2Service::Gateway < VCAP::Services::Base::Gateway

  def provisioner_class
    CF::UAA::OAuth2Service::Provisioner
  end

  def default_config_file
    config_base_dir = ENV["CLOUD_FOUNDRY_CONFIG_PATH"] || File.join(File.dirname(__FILE__), '..', '..', 'config')
    File.join(config_base_dir, 'oauth2_gateway.yml')
  end

  def additional_options
    @config[:cloud_controller_uri] ? {:cloud_controller_uri => @config[:cloud_controller_uri]} : {}
  end

end
