require 'rake'

desc "Run specs"
task "spec" => ["bundler:install:test", "test:spec"]

desc "Run specs using SimpleCov"
task "spec:rcov" => ["bundler:install:test", "test:spec:rcov"]

desc "Run ci using SimpleCov"
task "spec:ci" => ["bundler:install:test", "test:spec:ci"]

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
  task "spec" do |t|
    sh("cd spec && ../../base/bin/nats-util start && rake spec && ../../base/bin/nats-util stop")
  end

  task "spec:rcov" do |t|
    sh("cd spec && ../../base/bin/nats-util start && rake simcov && ../../base/bin/nats-util stop")
  end

  task "spec:ci" do |t|
    sh("cd spec && ../../base/bin/nats-util start && rake spec:ci && ../../base/bin/nats-util stop")
  end
end
