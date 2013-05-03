module VCAP::Services::Mysql::RDS
  class TransactionKiller
    def kill(thread_id, connection)
      connection.query("CALL mysql.rds_kill(#{thread_id})")
    end
  end
end
