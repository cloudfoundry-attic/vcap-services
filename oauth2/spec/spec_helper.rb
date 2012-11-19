#--
# Cloud Foundry 2012.02.03 Beta
# Copyright (c) [2009-2012] VMware, Inc. All Rights Reserved.
#
# This product is licensed to you under the Apache License, Version 2.0 (the "License").
# You may not use this product except in compliance with the License.
#
# This product includes a number of subcomponents with
# separate copyright notices and license terms. Your use of these
# subcomponents is subject to the terms and conditions of the
# subcomponent's license, as noted in the LICENSE file.
#++

require 'rspec'
require 'service'
require 'logger'
require 'vcap/common'

module SpecHelper

  def logger
    @logger ||= Logger.new(STDOUT)
  end

  def service_config
    return @service_config if @service_config
    config_file = ENV['CONFIG_FILE'] || CF::UAA::OAuth2Service::Gateway.new().default_config_file
    @service_config ||= YAML.load_file(config_file)
    @service_config = VCAP.symbolize_keys(@service_config)
    @service_config.delete(:mbus) unless ENV['CONFIG_FILE']
    @service_config = @service_config[:service]
  end
    
end
