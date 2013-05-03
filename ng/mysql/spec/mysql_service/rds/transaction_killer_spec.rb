require 'spec_helper'
require 'mysql_service/rds/transaction_killer'

describe VCAP::Services::Mysql::RDS::TransactionKiller do
  describe "killing transaction" do
    it "should use the connection to kill" do
      connection = stub
      connection.should_receive(:query).with('CALL mysql.rds_kill(1)')
      subject.kill(1, connection)
    end
  end
end
