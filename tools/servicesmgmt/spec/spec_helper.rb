require "rspec"
require "webrick"
require "rack/test"
require File.join(File.dirname(__FILE__), "..", "lib", "servicesmgmt")

INFO = {
  "access_token" => "eyJhbGciOiJIUzI1NiJ9.eyJqdGkiOiIzOTEzZjk2My0wZjQ0LTQ3OTgtOTFjYy01MWRmNGY3NWRhYTEiLCJ1c2VyX2lkIjoiNGVkYjIxMmEtZDY0Zi00YmM1LTk1ZGQtNGRlMGI1NWViZjE3Iiwic3ViIjoiNGVkYjIxMmEtZDY0Zi00YmM1LTk1ZGQtNGRlMGI1NWViZjE3IiwidXNlcl9uYW1lIjoic3JlQHZtd2FyZS5jb20iLCJlbWFpbCI6InNyZUB2bXdhcmUuY29tIiwic2NvcGUiOlsiY2xvdWRfY29udHJvbGxlci53cml0ZSIsIm9wZW5pZCIsImNsb3VkX2NvbnRyb2xsZXIucmVhZCJdLCJjbGllbnRfaWQiOiJzZXJ2aWNlc21nbXQiLCJjaWQiOiJzZXJ2aWNlc21nbXQiLCJpYXQiOjEzNjQ1MjcyOTYsImV4cCI6MTM2NDU3MDQ5NiwiaXNzIjoiaHR0cHM6Ly91YWEuY2Y5Ni5kZXYubGFzMDEudmNzb3BzLmNvbS9vYXV0aC90b2tlbiIsImF1ZCI6WyJvcGVuaWQiLCJjbG91ZF9jb250cm9sbGVyIl19.UaHkWqxcQvd3YKv_NRK_fBCgUmiIemmAQtZ6e4UfHEo",
  "token_type" => "bearer",
  "refresh_token" => "eyJhbGciOiJIUzI1NiJ9.eyJqdGkiOiIxZTFiOWQwZS04MjhiLTQ0NTAtYjNmYS00M2E3ODczMjgxNmEiLCJzdWIiOiI0ZWRiMjEyYS1kNjRmLTRiYzUtOTVkZC00ZGUwYjU1ZWJmMTciLCJ1c2VyX25hbWUiOiJzcmVAdm13YXJlLmNvbSIsInNjb3BlIjpbImNsb3VkX2NvbnRyb2xsZXIud3JpdGUiLCJvcGVuaWQiLCJjbG91ZF9jb250cm9sbGVyLnJlYWQiXSwiaWF0IjoxMzY0NTI3Mjk2LCJleHAiOjEzNjcxMTkyOTYsImNpZCI6InNlcnZpY2VzbWdtdCIsImlzcyI6Imh0dHBzOi8vdWFhLmNmOTYuZGV2LmxhczAxLnZjc29wcy5jb20vb2F1dGgvdG9rZW4iLCJhdWQiOlsiY2xvdWRfY29udHJvbGxlci53cml0ZSIsIm9wZW5pZCIsImNsb3VkX2NvbnRyb2xsZXIucmVhZCJdfQ.yZK5bVZ3v00DNbapLcdwClb4qtittOxTDNvUYFZPfXo",
  "expires_in" => 43199,
  "scope" => "cloud_controller.write openid cloud_controller.read",
  "jti" => "3913f963-0f44-4798-91cc-51df4f75daa1"
}

TEST_TOKENINFO = CF::UAA::TokenInfo.new(INFO)

class ServicesMgmtWithToken < ServicesMgmt
  def need_token?(token)
    false
  end
end

class MyHash < Hash
  def initialize
    super{|h, k| h[k] = MyHash.new}
  end
end

def create_http_server(path, body)
  t = Thread.new do
    server = WEBrick::HTTPServer.new :Port => 12342
    server.mount_proc path do |req, res|
      res.body = body
    end
    server.start
  end
  sleep 1
  yield
  t.kill
end
