source :rubygems

gem 'eventmachine', :git => 'git://github.com/cloudfoundry/eventmachine.git', :branch => 'release-0.12.11-cf'
gem "em-http-request"
gem "ruby-hmac"
gem "uuidtools"
gem "datamapper", ">= 0.10.2"
gem "dm-sqlite-adapter"
gem "do_sqlite3"
gem "sinatra"
gem "thin"

gem 'vcap_common', :require => ['vcap/common', 'vcap/component'], :git => 'git://github.com/cloudfoundry/vcap-common.git', :ref => 'b7e3508eb9'
gem 'vcap_logging', :require => ['vcap/logging'], :git => 'git://github.com/cloudfoundry/common.git', :ref => 'b96ec1192'
gem 'vcap_services_base', :git => 'git://github.com/cloudfoundry/vcap-services-base.git', :ref => '1077d35ef7'
gem 'warden-client', :require => ['warden/client'], :git => 'git://github.com/cloudfoundry/warden.git', :ref => '84dcc3acb0'

group :test do
  gem "rake"
  gem "rspec"
  gem "simplecov"
  gem "simplecov-rcov"
  gem "ci_reporter"
end
