require 'sinatra'
require 'thin'
require 'httpclient'

class FakeAppDirectServer < Sinatra::Base
  get '/*' do
    client = HTTPClient.new
    response = client.get(['https://dev3cloudfoundry.appdirect.com', *params[:splat]].join('/'))

    puts "params: #{params.inspect}"
    puts "response: #{response.inspect}"

    status  response.status
    headers 'Content-Type' => 'application/json'
    body    response.body
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

