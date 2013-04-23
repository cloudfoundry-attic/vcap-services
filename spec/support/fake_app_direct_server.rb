require 'sinatra'
require 'thin'
require 'httpclient'

class FakeAppDirectServer < Sinatra::Base
  get '/api/custom/cloudfoundry/v1/offerings' do
    response = File.read File.expand_path(File.join('assets', 'fake_app_direct', 'custom_api_listing.json'))
    puts "Fake response: #{response}"

    status  200
    headers 'Content-Type' => 'application/json'
    body response
  end

  get '/api/marketplace/v1/products/8' do
    response = File.read File.expand_path(File.join('assets', 'fake_app_direct', 'public_mongo_details.json'))
    puts "Fake response: #{response}"
    status  200
    headers 'Content-Type' => 'application/json'
    body response
  end

  get '/api/marketplace/v1/products/47' do
    response = File.read File.expand_path(File.join('assets', 'fake_app_direct', 'public_sendgrid_details.json'))
    puts "Fake response: #{response}"
    status  200
    headers 'Content-Type' => 'application/json'
    body response
  end

  get '/api/marketplace/v1/products/50' do
    status 404
  end

  get '/*' do
    raise "Requesting a path that is not defined #{params[:splat].join('/')}"
  end

end

module FakeAppDirectRunner
  def self.start
    Thin::Server.start("localhost", 9999, FakeAppDirectServer.new)
  end
end

