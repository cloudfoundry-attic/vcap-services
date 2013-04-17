class AppDirectEndpointStubs
  HOST = "127.0.0.1"
  PORT = 15000

  def self.get_endpoint
    "http://#{HOST}:#{PORT}"
  end

  def self.create_mock_endpoint(scenario = "")
    mep = MockEndpoint.new(scenario)
    mep.start
    mep
  end

  def self.load_fixture(filename, resp = '{}')
    File.read("#{File.dirname(__FILE__)}/app_direct_endpoint_fixtures/#{filename}") rescue resp
  end

  class MockEndpoint
    def initialize(scenario)
      Thin::Logging.silent = true

      @server = Thin::Server.new("#{HOST}", PORT, Handler.new(scenario))
    end

    def start
      Thread.new { @server.start }
      while !@server.running?
        sleep 0.1
      end
    end

    def stop
      @server.stop if @server
    end

    class Handler < Sinatra::Base

      def initialize(s)
        @scenario = s
      end

      get "/*" do
        path = params[:splat]

        load_fixture("get", path[0])
      end

      post "/*" do
        path = params[:splat]
        data = JSON.parse(request.body.read)

        load_fixture("post", path[0])
      end

      delete "/*" do
        path = params[:splat]
        data = JSON.parse(request.body.read)

        load_fixture("post", path[0])
      end

      helpers do

        def load_fixture(verb, path, resp = '{}')
          # Remove api/ from path
          path = path[4, path.size-4]
          AppDirectEndpointStubs.load_fixture("#{@scenario}#{path}/#{verb}_response.json", resp)
        end
      end

    end
  end
end
