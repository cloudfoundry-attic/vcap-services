require 'sinatra'
require 'thin'
require 'httpclient'
require 'json'

class FakeAppDirectServer < Sinatra::Base
  get '/api/custom/cloudfoundry/v1/offerings' do
    response = File.read File.expand_path(File.join('assets', 'fake_app_direct', 'custom_api_listing.json'))
    status  200
    headers 'Content-Type' => 'application/json'
    body response
  end

  get '/api/marketplace/v1/products/8' do
    response = File.read File.expand_path(File.join('assets', 'fake_app_direct', 'public_mongo_details.json'))
    status  200
    headers 'Content-Type' => 'application/json'
    body response
  end

  get '/api/marketplace/v1/products/47' do
    response = File.read File.expand_path(File.join('assets', 'fake_app_direct', 'public_sendgrid_details.json'))
    status  200
    headers 'Content-Type' => 'application/json'
    body response
  end

  get '/api/marketplace/v1/products/50' do
    status 404
  end

  post '/api/custom/cloudfoundry/v1/services' do
    post_body = JSON.parse(request.body.read)
    provision_service(post_body)
    puts "provisioned with request #{post_body.inspect}"

    status  201
    headers 'Content-Type' => 'application/json'
    service_external_id = rand(100)
    body JSON.dump(
      "uuid"=>"575fd20b-69b7-410b-aba7-2dad5d8f1ffa",
      "id"=>"706db309-98f2-421a-b9c9-ec42f0699c7f",
      "space" => post_body.fetch('space'),
      "offering" => {
        "label" => post_body.fetch('offering').fetch('label'),
        "provider" => post_body.fetch('offering').fetch('provider'),
        "version"=>"n/a",
        "description"=>"Service description",
        "plans"=>[],
        "external_id" => service_external_id.to_s,
        "info_url"=>"https://dev3cloudfoundry.appdirect.com/apps/#{service_external_id}"
      },
      "configuration" => {
        "plan" => {
          "external_id" => post_body.fetch('configuration').fetch('plan').fetch('external_id'),
          "name" => 'name?',
          "id"=>"plan_name",
          "description"=>"Plan description",
          "free"=>true,
        }
      },
      "credentials" => {
        "dummy"=>"value"
      }
    )
  end

  get '/test/provisioned_services' do
    headers 'Content-Type' => 'application/json'
    body JSON.dump(provisioned_services.tap{|j| puts "json: #{j.inspect}"})
  end

  def provision_service(request_json)
    provisioned_services << request_json
  end

  def provisioned_services
    @@services ||= []
  end

  post '/*' do
    puts "Unknown POST request #{params[:splat].join('/')}"
    raise "Requesting a path that is not defined #{params[:splat].join('/')}"
  end

  get '/*' do
    puts "Unknown GET request #{params[:splat].join('/')}"
    raise "Requesting a path that is not defined #{params[:splat].join('/')}"
  end
end

module FakeAppDirectRunner
  def self.start
    Thin::Server.start("localhost", 9999, FakeAppDirectServer.new)
  end
end

