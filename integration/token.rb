require "uaa/token_coder"

token_coder = CF::UAA::TokenCoder.new(:audience_ids => "cloud_controller",
                                      :skey => "tokensecret", :pkey => nil)

user_token = token_coder.encode(
  :user_id => (rand * 1_000_000_000).ceil,
  :client_id => "vmc",
  :email => "sre@vmware.com",
  :scope => %w[cloud_controller.admin]
)

puts "bearer #{user_token}"
