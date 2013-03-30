require "sinatra/base"
require "json"
require "base64"

module Sinatra
  module SSORabbitmq
    def self.registered(app)
      app.post "/rabbitmq" do
        id = params[:id]
        credentials = JSON.parse(params[:credentials])
        host = credentials["host"]
        port = credentials["admin_port"]
        username = credentials["monit_user"]
        password = credentials["monit_pass"]

        auth = Base64.encode64("#{username}:#{password}").chop
        response.set_cookie("auth", auth)
        session[:svc] = {
          :id => id,
          :host => host,
          :port => port,
          :username => username,
          :password => password,
        }
        redirect "/proxy/#{id}/"
      end
    end
  end
end
