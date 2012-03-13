module VCAP
  module Services
    module Postgresql

      class Node
        class Provisionedservice
          include DataMapper::Resource
          property :name,       String,   :key => true
          # property plan is deprecated. The instances in one node have same plan.
          property :plan,       Integer, :required => true
          property :quota_exceeded,  Boolean, :default => false
          has n, :bindusers
        end

        class Binduser
          include DataMapper::Resource
          property :user,       String,   :key => true
          property :sys_user,    String,    :required => true
          property :password,   String,   :required => true
          property :sys_password,    String,    :required => true
          property :default_user,  Boolean, :default => false
          belongs_to :provisionedservice
        end

        def self.setup_datamapper(sym, orm_db, auto_upgrade=true)
          DataMapper.setup(sym, orm_db)
          DataMapper::auto_upgrade! if auto_upgrade
        end

      end
    end
  end
end
