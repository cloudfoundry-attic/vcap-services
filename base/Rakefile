require 'rake'
require 'bundler'

desc "Run specs"
task "spec" => ["bundler:install:test", "test:spec"]

desc "Run specs using SimpleCov"
task "spec:rcov" => ["bundler:install:test", "test:spec:rcov"]

desc "Run ci using SimpleCov"
task "spec:ci" => ["bundler:install:test", "test:spec:ci"]

namespace "bundler" do
  gem_helper = Bundler::GemHelper.new(Dir.pwd)
  desc "Build gem package"
  task "build" do
    gem_helper.build_gem
  end

  desc "Install gems"
  task "install" do
    sh("bundle install")
    gem_helper.install_gem
  end

  desc "Install gems for test"
  task "install:test" do
    sh("bundle install --without development production")
    gem_helper.install_gem
  end

  desc "Install gems for production"
  task "install:production" do
    sh("bundle install --without development test")
    gem_helper.install_gem
  end

  desc "Install gems for development"
  task "install:development" do
    sh("bundle install --without test production")
    gem_helper.install_gem
  end
end

namespace "test" do
  task "spec" do |t|
    sh("cd spec && ../bin/nats-util start && rake spec && ../bin/nats-util stop")
  end

  task "spec:rcov" do |t|
    sh("cd spec && ../bin/nats-util start && rake simcov && ../bin/nats-util stop")
  end

  task "spec:ci" do |t|
    sh("cd spec && ../bin/nats-util start && rake spec:ci && ../bin/nats-util stop")
  end
end
