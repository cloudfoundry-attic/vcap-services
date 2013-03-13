$LOAD_PATH.unshift(File.dirname(__FILE__))
require "sinatra/base"
require "sinatra/config_file"
require "erb"
require "rack-proxy"
require "omniauth-uaa-oauth2"
require "vcap/logging"
require "cfoundry"
require "sso_rabbitmq"

class ServicesMgmt < Sinatra::Base
  set :root, File.expand_path("../../", __FILE__)
  set :protection, :except => [:json_csrf]

  register Sinatra::SSORabbitmq
  register Sinatra::ConfigFile
  config_file ENV["CONFIG_FILE"] || File.expand_path("../../config/servicesmgmt.yml", __FILE__)

  use Rack::Session::Pool
  use OmniAuth::Builder do
    provider :cloudfoundry,
      @@id,
      @@secret,
      {:auth_server_url => @@auth_server, :token_server_url => @@token_server}
  end

  before do
    unprotected = ["/auth/cloudfoundry/callback"]
    unless unprotected.include?(request.path_info) then
      redirect "/auth/cloudfoundry" if need_token?(session[:auth])
    end
  end

  def initialize
    super
    @backend = Rack::Proxy.new
    @@id = settings.id
    @@secret = settings.secret
    @@token_server = settings.token_server
    @@auth_server = settings.auth_server
    @@base_url = settings.cloud_controller_uri
    VCAP::Logging.setup_from_config(settings.logging)
    @logger = VCAP::Logging.logger("#{settings.id}_#{settings.index}")
    OmniAuth.config.logger = @logger
    @logger.info("Init #{settings.id}")
  end

  def need_token?(auth)
    return true unless auth
    token = auth[:token]
    token && token.expires_soon?
  end

  #######
  # Handler
  #######
  get "/" do
    username = ""
    svcs = []

    begin
      username = session[:auth][:name]
      token = session[:auth][:token]

      @logger.debug("Login as #{username}")
      client = CFoundry::V2::Client.new(@@base_url, token)
      res = client.service_instances(:depth => 2)

      res.each do |ins|
        data = ins.manifest
        svc = {}
        svc[:id] = data[:entity][:credentials][:name]
        svc[:name] = data[:entity][:name]
        svc[:plan] = data[:entity][:gateway_data][:plan]
        svc[:version] = data[:entity][:gateway_data][:version]
        svc[:label] = data[:entity][:service_plan][:entity][:service][:entity][:label]
        svc[:credentials] = data[:entity][:credentials]
        svcs << svc
      end
    rescue => e
      @logger.error("Exception on index page: #{e}")
    end

    erb :index, :locals => {:username => username, :svcs => svcs}
  end

  [:get, :post, :put, :delete, :head, :options].each do |verb|
    send(verb, "/proxy/:id/*") do
      id = params["id"]
      env = request.env

      begin
        svc = session[:svc]
        @logger.debug("#{verb} #{request.path} with #{svc}")
        if svc && svc[:id] == id
          env["HTTP_HOST"] = "#{svc[:host]}:#{svc[:port]}"
          env["PATH_INFO"] = env["REQUEST_PATH"] = request.path.gsub(/^\/proxy\/#{id}/, "")
          status, headers, body = @backend.relay_request(env)
        else
          raise "Not found service in session"
        end
      rescue => e
        @logger.warn("Proxy page error : #{e}")
        status, headers, body = 404, nil, "Not found"
      end
    end
  end

  get "/auth/cloudfoundry/callback" do
    auth = request.env["omniauth.auth"]
    @logger.debug("Get callback for auth: #{auth}")
    token = CFoundry::AuthToken.from_hash(
      :token => "bearer #{auth[:credentials][:token]}",
      :refresh_token => auth[:credentials][:refresh_token]
    )
    info = auth[:info].to_hash
    name = info["name"]
    session[:auth] = {:name => name, :token => token}
    redirect "/"
  end
end

class Rack::Proxy
  def relay_request(env)
    perform_request(env)
  end
end
