# Copyright (c) 2009-2011 VMware, Inc.
module VCAP
  module Services
    module Backup
    end
  end
end

$LOAD_PATH.unshift File.dirname(__FILE__)
require 'rotator'

class VCAP::Services::Backup::Manager

  def initialize(options)
    @logger = options[:logger]
    @once = options[:once]
    @daemon = options[:daemon]
    @logger.info("#{self.class}: Initializing")
    @wakeup_interval = options[:wakeup_interval]
    @root = File.join(options[:root], "backups")
    @tasks = [
      VCAP::Services::Backup::Rotator.new(self, options[:rotation])
    ]
    @enable = options[:enable]
  end

  attr_reader :root
  attr_reader :logger

  def start
    @logger.info("#{self.class}: Starting")
    if @daemon
      pid = fork
      if pid
        @logger.info("#{self.class}: Forked process #{pid}")
        Process.detach(pid)
      else
        @logger.info("#{self.class}: Daemon starting")
        loop {
          sleep @wakeup_interval
          run
        }
      end
    elsif @once
      run
    else
      loop {
        sleep @wakeup_interval
        run
      }
    end
  end

  def run
    if @enable
      begin
        @logger.info("#{self.class}: Running")
        @tasks.each { |task|
          unless task.run
            @logger.warn("#{self.class}: #{task.class} failed")
          end
        }
      rescue => x
        # tasks should catch their own exceptions, but just in case...
        @logger.error("#{self.class}: Exception while running: #{x.to_s}")
      end
    else
      @logger.info("#{self.class}: Not enabled")
    end
  end

  def time
    Time.now.to_i
  end

end
