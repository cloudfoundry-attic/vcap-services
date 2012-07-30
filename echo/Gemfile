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

gem 'vcap_common', :require => ['vcap/common', 'vcap/component'], :git => 'git://github.com/cloudfoundry/vcap-common.git', :ref => 'b0cc19d5'
gem 'vcap_logging', :require => ['vcap/logging'], :git => 'git://github.com/cloudfoundry/common.git', :ref => 'b96ec1192'
gem 'vcap_services_base', :git => 'git://github.com/cloudfoundry/vcap-services-base.git', :ref => 'b8a47d1b'
gem 'warden-client', :require => ['warden/client'], :git => 'git://github.com/cloudfoundry/warden.git', :ref => 'd8334ce8a3'
gem 'warden-protocol', :require => ['warden/protocol'], :git => 'git://github.com/cloudfoundry/warden.git', :ref => 'd8334ce8a3'

group :test do
  gem "rake"
  gem "rspec"
  gem "simplecov"
  gem "simplecov-rcov"
  gem "ci_reporter"
end
