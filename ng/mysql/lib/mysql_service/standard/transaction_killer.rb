module VCAP::Services::Mysql::Standard
  class TransactionKiller
    def kill(id, connection)
      connection.query("KILL #{id}")
    end
  end
end
