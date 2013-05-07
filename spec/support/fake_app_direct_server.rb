require 'sinatra'
require 'thin'
require 'httpclient'
require 'json'
require 'securerandom'

class FakeAppDirectServer < Sinatra::Base
  get '/api/custom/cloudfoundry/v1/offerings' do
    response = File.read File.expand_path(File.join('assets', 'fake_app_direct', 'custom_api_listing.json'))
    status  200
    headers 'Content-Type' => 'application/json'
    body response
  end

  get '/api/marketplace/v1/products/7' do
    status  404
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
    uuid = SecureRandom.uuid
    provision_service(post_body.merge('uuid' => uuid))
    puts "provisioned with request #{post_body.inspect}"

    status  201
    headers 'Content-Type' => 'application/json'
    service_external_id = rand(100)
    body JSON.dump(
      "uuid" => uuid,
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

  delete '/api/custom/cloudfoundry/v1/services/:service_guid' do |service_guid|
    deprovision_service(service_guid)
    status 200
  end

  get '/test/provisioned_services' do
    headers 'Content-Type' => 'application/json'
    body JSON.dump(provisioned_services)
  end

  get '/test/deprovisioned_services' do
    headers 'Content-Type' => 'application/json'
    body JSON.dump(deprovisioned_services)
  end

  def provision_service(request_json)
    provisioned_services << request_json
  end

  def provisioned_services
    @@provisioned_services ||= []
  end

  def deprovision_service(instance_guid)
    deprovisioned_services << instance_guid
  end

  def deprovisioned_services
    @@deprovisioned_services ||= []
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

