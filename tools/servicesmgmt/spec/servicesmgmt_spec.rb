$LOAD_PATH.unshift(File.dirname(__FILE__))
require "spec_helper"

describe "ServicesMgmt" do
  include Rack::Test::Methods

  context "authentication" do
    def app
      ServicesMgmt
    end

    it "should be redirected without token" do
      get "/"
      last_response.should be_redirect
    end

    it "should be able to parse & set token in omniauth callback" do
      OmniAuth::Strategies::Cloudfoundry.any_instance.stub(:build_access_token){TEST_TOKENINFO}
      OmniAuth::Strategies::Cloudfoundry.any_instance.stub(:expired?){false}
      OmniAuth::Strategies::Cloudfoundry.any_instance.stub(:raw_info){{}}
      get "/auth/cloudfoundry/callback"
      last_request.env["rack.session"]["auth"][:token].refresh_token.should == INFO["refresh_token"]
    end
  end

  context "tests with token verified" do
    def app
      ServicesMgmtWithToken
    end

    it "should render index with instances" do
      ins_name = "test_instance"
      user = "test@abc.com"

      Instance = Struct.new(:manifest)
      CFoundry::V2::Client.any_instance.stub(:service_instances) do
        ins = Instance.new(MyHash.new)
        ins.manifest[:entity][:service_plan][:entity][:service][:entity][:label] = "rabbitmq"
        ins.manifest[:entity][:name] = "test_instance"
        [ins]
      end
      get "/", {}, "rack.session" => {:auth => {:name => user}}
      last_response.should be_ok
      last_response.body.should =~ /#{ins_name}/
      last_response.body.should =~ /#{user}/
    end

    it "should do basic authentication for rabbit" do
      user = "testuser"
      pass = "testpass"
      cred = {
        "host" => "localhost",
        "admin_port" => 1234,
        "monit_user" => user,
        "monit_pass" => pass,
      }.to_json
      auth = CGI.escape(Base64.encode64("#{user}:#{pass}").chop)

      post "/rabbitmq", {:id => "testid", :credentials => cred}
      last_response.should be_redirect
      last_response["Set-Cookie"].should =~ /#{auth}/
    end

    it "should relay the request to backend server" do
      path = "/misc"
      body = "abc"

      create_http_server(path, body) do
        get(
          "/proxy/testid#{path}",
          {},
          "rack.session" => {
            :svc => {
              :host => "localhost",
              :port => 12342,
              :id => "testid"
            }
          }
        )
        last_response.body.should == body
      end
    end

    it "should refuse to relay the request without session info" do
      path = "/misc"
      body = "abc"

      create_http_server(path, body) do
        get "/proxy/testid#{path}"
        last_response.status.should == 404
      end
    end

    it "should refuse to relay the request whose instance id does not match" do
      path = "/misc"
      body = "abc"

      create_http_server(path, body) do
        get(
          "/proxy/testabc#{path}",
          {},
          "rack.session" => {
            :svc => {
              :host => "localhost",
              :port => 12342,
              :id => "testid"
            }
          }
        )
        last_response.status.should == 404
      end
    end
  end
end
