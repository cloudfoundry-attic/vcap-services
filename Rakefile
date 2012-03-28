SERVICES_DIR = %w(atmos filesystem mongodb mysql neo4j postgresql rabbit redis service_broker vblob tools/backup/manager)

desc "Run integration tests."
task "tests" do |t|
  system "cd tests; bundle exec rake tests"
end

namespace "bundler" do
  desc "Update base gem"
  task "update_base" do
    system "cd base && rm -rf pkg && rake bundler:install"
    SERVICES_DIR.each do |dir|
      puts ">>>>>>>> enter #{dir}"
      system "rm -f #{dir}/vendor/cache/vcap_services_base-*.gem && cp base/pkg/vcap_services_base-*.gem #{dir}/vendor/cache && cd #{dir} && bundle install --local"
    end
  end
end
