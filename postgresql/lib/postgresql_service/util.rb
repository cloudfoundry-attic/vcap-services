# Copyright (c) 2009-2011 VMware, Inc.
module VCAP
  module Services
    module Postgresql

      # FIXME this should probably go into common
      module Util
        VALID_CREDENTIAL_CHARACTERS = ("A".."Z").to_a + ("a".."z").to_a + ("0".."9").to_a

        def parse_property(hash, key, type, options = {})
          obj = hash[key]
          if obj.nil?
            raise "Missing required option: #{key}" unless options[:optional]
            nil
          elsif type == Range
            raise "Invalid Range object: #{obj}" unless obj.kind_of?(Hash)
            first, last = obj["first"], obj["last"]
            raise "Invalid Range object: #{obj}" unless first.kind_of?(Integer) and last.kind_of?(Integer)
            Range.new(first, last)
          else
            raise "Invalid #{type} object: #{obj}" unless obj.kind_of?(type)
            obj
          end
        end

        def create_logger(logdev, rotation, level)
          if String === logdev
            dir = File.dirname(logdev)
            FileUtils.mkdir_p(dir) unless File.directory?(dir)
          end
          logger = Logger.new(logdev, rotation)
          logger.level = case level
            when "DEBUG" then Logger::DEBUG
            when "INFO" then Logger::INFO
            when "WARN" then Logger::WARN
            when "ERROR" then Logger::ERROR
            when "FATAL" then Logger::FATAL
            else Logger::UNKNOWN
          end
          logger
        end

        def generate_credential(length=12)
          Array.new(length) { VALID_CREDENTIAL_CHARACTERS[rand(VALID_CREDENTIAL_CHARACTERS.length)] }.join
        end

      end
    end
  end
end
