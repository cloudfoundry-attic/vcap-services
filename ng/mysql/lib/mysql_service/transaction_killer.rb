require_relative 'rds/transaction_killer'
require_relative 'standard/transaction_killer'

module VCAP::Services::Mysql
  class TransactionKiller
    def self.build(provider)
      if provider == "rds"
        RDS::TransactionKiller.new
      else
        Standard::TransactionKiller.new
      end
    end
  end
end
