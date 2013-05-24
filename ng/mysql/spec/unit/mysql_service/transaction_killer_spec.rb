require 'spec_helper'
require 'mysql_service/transaction_killer'

describe VCAP::Services::Mysql::TransactionKiller do
  context "provider is RDS" do
    it "returns a rds killer" do
      described_class.build('rds').
        should be_a(VCAP::Services::Mysql::RDS::TransactionKiller)
    end
  end

  context "provider is not specified" do
    it "returns the standard killer" do
      described_class.build(nil).
        should be_a(VCAP::Services::Mysql::Standard::TransactionKiller)
    end
  end
end
