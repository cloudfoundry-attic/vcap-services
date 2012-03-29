# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))

module VCAP
  module Services
    module MongoDB
      module Common
        def service_name
          "MongoaaS"
        end
      end
    end
  end
end

