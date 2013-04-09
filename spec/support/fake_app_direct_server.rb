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

  get '/*' do
    raise "Requesting a path that is not defined #{params[:splat].join('/')}"
  end

end

module FakeAppDirectRunner
  def self.start
    # require 'webmock'
    # require 'vcr'
    # VCR.configure do |c|
      # c.cassette_library_dir = 'assets/fake_app_direct'
      # c.hook_into :webmock
    # end
    # VCR.use_cassette('catalog_response') do
      Thin::Server.start("localhost", 9999, FakeAppDirectServer.new)
    # end
  end
end

