require 'rake'

task "default" => "spec"

desc "Run specs"
task "spec" => ["test:spec"]

desc "Run specs using RCov"
task "spec:rcov" => ["test:spec:rcov"]

desc "Run ci using SimpleCov"
task "spec:ci" => ["test:spec:ci"]

namespace "bundler" do
  desc "Install gems"
  task "install" do
    sh("bundle install")
  end

  desc "Install gems for test"
  task "install:test" do
    sh("bundle install --without development production")
  end

  desc "Install gems for production"
  task "install:production" do
    sh("bundle install --without development test")
  end

  desc "Install gems for development"
  task "install:development" do
    sh("bundle install --without test production")
  end
end

namespace "test" do
  def run_spec
    sh "nats-server &"
    Dir.chdir("spec"){ yield }
    sh "pkill -f nats-server"
  end

  task "spec" do |t|
    run_spec{ sh "rake spec" }
  end

  task "spec:rcov" do |t|
    run_spec{ sh "rake simcov" }
  end

  task "spec:ci" do |t|
    run_spec{ sh "rake spec:ci" }
  end
end
