module CcngClient
  def ccng_post(resource_path, body_hash)
    make_ccng_request(:post, resource_path, body_hash)
  end

  def ccng_put(resource_path, body_hash)
    make_ccng_request(:put, resource_path, body_hash)
  end

  def ccng_get(resource_path)
    make_ccng_request(:get, resource_path)
  end

  def ccng_auth_token
    token_coder = CF::UAA::TokenCoder.new(:audience_ids => "cloud_controller",
                                          :skey => "tokensecret", :pkey => nil)

    user_token = token_coder.encode(
      :user_id => user_guid,
      :client_id => "vmc",
      :email => "sre@vmware.com",
      :scope => %w[cloud_controller.admin]
    )

    "bearer #{user_token}"
  end

  private
  def make_ccng_request(method, resource_path, body_hash=nil)
    uri = URI.parse("http://127.0.0.1:8181/")
    uri.path = resource_path
    response = client.public_send(method,
                                  uri,
                                  header: { "AUTHORIZATION" => ccng_auth_token },
                                  body: Yajl::Encoder.encode(body_hash)
                                 )
    raise "Unexpected response from #{resource_path}: #{response.inspect}" unless response.ok?
    Yajl::Parser.parse(response.body)
  end

  def client
    HTTPClient.new
  end
end
