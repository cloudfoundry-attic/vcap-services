require 'spec_helper'
require 'mysql_service/standard/transaction_killer'

describe VCAP::Services::Mysql::Standard::TransactionKiller do
  describe "killing transaction" do
    it "should use the connection to kill" do
      connection = stub
      connection.should_receive(:query).with('KILL 1')
      subject.kill(1, connection)
    end
  end
end
