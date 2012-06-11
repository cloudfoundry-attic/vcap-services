SERVICES_DIR = %w(atmos filesystem memcached mongodb mysql neo4j postgresql rabbit redis service_broker vblob tools/backup/manager)

desc "Run integration tests."
task "tests" do |t|
  system "cd tests; bundle exec rake tests"
end

namespace "bundler" do
  def exec_in_svc_dir
    SERVICES_DIR.each do |dir|
      puts ">>>>>>>> enter #{dir}"
      Dir.chdir(dir) do
        yield dir
      end
    end
  end

  desc "Update Gemfile"
  task :update!, :oref, :nref do |t, args|
    exec_in_svc_dir { |_| sh "sed -i \"s/#{args[:oref]}/#{args[:nref]}/g\" Gemfile && bundle install" }
  end

  desc "Dry run update Gemfile"
  task :update, :oref, :nref do |t, args|
    exec_in_svc_dir { |_| sh "sed \"s/#{args[:oref]}/#{args[:nref]}/g\" Gemfile" }
  end
end
