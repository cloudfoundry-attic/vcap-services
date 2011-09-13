#!/usr/bin/env ruby
ENV["BUNDLE_GEMFILE"] ||= File.expand_path("../Gemfile", __FILE__)
require "rubygems"
require "bundler/setup"
require 'sinatra'
require 'thin'
require 'datamapper'
require 'logger'
require 'yajl'

# VCAP environment
port = ENV['VMC_APP_PORT']
port ||= 8082

class TestApp < Sinatra::Base
  set :public, File.join(File.dirname(__FILE__) , '/static')
  set :views, File.join(File.dirname(__FILE__) , '/template')

  class User
    include DataMapper::Resource
    property :id, Serial, :key => true
    property :name, String, :required => true
  end

  def initialize(opts)
    super
    @opts = opts
    @logger = Logger.new(STDOUT, 'daily')
    @logger.level = Logger::DEBUG
    @db= false
    if @opts[:mysql]
      @db= true
      DataMapper.setup(:default, @opts[:mysql])
      DataMapper::auto_upgrade!
    end
  end

  not_found do
    halt 404
  end

  error do
    @logger.error("Error: #{env['sinatra.erro']}")
    halt 500
  end

  get '/' do
    'It works.'
  end

  before '/user/*' do
    if not @db
      halt 500, "database not enabled."
    end
  end

  get '/user/:id' do
    @logger.debug("Get user #{params[:id]}")
    user = User.get(params[:id])
    if user
      user.name
    else
      halt 404
    end
  end

  get '/user' do
    users = User.all
    res = ""
    users.each do |user|
      name = user.name
      res += "#{name}\n"
    end
    res
  end

  post '/user' do
    request.body.rewind
    name = request.body.read
    @logger.debug("Create a user #{name}")
    user = User.new
    user.name = name
    if not user.save
      @logger.error("Can't save to db:#{user.errors.pretty_inspect}")
      halt 500
    else
      redirect ("/user/#{user.id}")
    end
  end

end

config = {}
svcs = ENV['VMC_SERVICES']
if svcs
  # override db config if VMC_SERVICE atmos service is supplied.
  svcs = Yajl::Parser.parse(svcs)
  svcs.each do |svc|
    if svc["name"] =~ /^mysql/
      opts = svc["options"]
      user,passwd,host,db,db_port = %w(user password hostname name port).map {|key|
        opts[key]}
      conn_string="mysql://#{user}:#{passwd}@#{host}:#{db_port}/#{db}"
      config[:mysql] = conn_string
    end
  end
end

puts "Config: #{config.inspect}"
instance = TestApp.new(config)
Thin::Server.start('0.0.0.0', port , instance)
