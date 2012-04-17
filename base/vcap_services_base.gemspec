$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), "lib"))
require "base/version"

Gem::Specification.new do |s|
  s.name         = "vcap_services_base"
  s.version      = VCAP::Services::Base::VERSION
  s.platform     = Gem::Platform::RUBY
  s.summary      = "VCAP services base module"
  s.description  = s.summary
  s.files        = `git ls-files`.split("\n")
  s.require_path = [ "lib" ]
  s.author       = "CloudFoundry Services Team"
  s.email        = "cf-services-eng-cn@vmware.com"

  s.add_dependency "nats", "~> 0.4.22.beta.8"
  s.add_dependency "datamapper", "~> 1.1.0"
  s.add_dependency "do_sqlite3", "~> 0.10.3"
  s.add_dependency "eventmachine", "~> 0.12.11.cloudfoundry.3"
  s.add_dependency "eventmachine_httpserver", "~> 0.2.1"
  s.add_dependency "json", "~> 1.4.6"
  s.add_dependency "uuidtools", "~> 2.1.2"
  s.add_dependency "ruby-hmac", "~> 0.4.0"
  s.add_dependency "em-http-request", "~> 0.3.0"
  s.add_dependency "sinatra", "~> 1.2.3"
  s.add_dependency "thin", "~> 1.3.1"
  s.add_dependency "vcap_common", ">= 1.0.8"
  s.add_dependency "vcap_logging", ">=0.1.3"
  s.add_dependency "resque", "~> 1.20"
  s.add_dependency "resque-status", "~> 0.3.2"
  s.add_dependency "curb", "~> 0.7.16"
end
