source "http://rubygems.org"

gem 'eventmachine', :git => 'git://github.com/cloudfoundry/eventmachine.git', :branch => 'release-0.12.11-cf'
gem "em-http-request"
gem "nats", '>= 0.4.8'
gem "ruby-hmac"
gem "uuidtools"
gem "datamapper", "= 1.1.0"
gem "dm-sqlite-adapter"
gem "do_sqlite3"
gem "sinatra", "~> 1.2.3"
gem "thin"

gem 'vcap_common', :require => ['vcap/common', 'vcap/component'], :git => 'git://github.com/cloudfoundry/vcap-common.git'
gem 'vcap_logging', :require => ['vcap/logging'], :git => 'git://github.com/cloudfoundry/common.git', :ref => 'b96ec1192'
gem 'vcap_services_base', :git => 'git://github.com/cloudfoundry/vcap-services-base.git', :ref => 'f5ca1ed'
gem 'warden-client', :require => ['warden/client'], :git => 'git://github.com/cloudfoundry/warden.git', :ref => 'fe6cb51'
gem 'warden-protocol', :require => ['warden/protocol'], :git => 'git://github.com/cloudfoundry/warden.git', :ref => 'fe6cb51'

group :test do
  gem "rake"
  gem "rspec"
  gem "simplecov"
  gem "simplecov-rcov"
  gem "ci_reporter"
end
