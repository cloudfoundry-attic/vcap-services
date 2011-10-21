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
    @shutdown = false
    @run_lock = Mutex.new
  end

  attr_reader :root
  attr_reader :logger

  def exit_fun
    @logger.info("Terminating the application")
    @shutdown = true
    Thread.new do
      @run_lock.synchronize { exit }
    end
  end

  def shutdown?
    @shutdown
  end

  def start
    @logger.info("#{self.class}: Starting")
    trap("TERM"){ exit_fun }
    trap("INT"){ exit_fun }
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
      @run_lock.synchronize do
        begin
          @logger.info("#{self.class}: Running")
          @tasks.each do |task|
            unless task.run
              @logger.warn("#{self.class}: #{task.class} failed")
            end
          end
        rescue => x
          # tasks should catch their own exceptions, but just in case...
          @logger.error("#{self.class}: Exception while running: #{x.to_s}")
        rescue Interrupt
          @logger.info("#{self.class}: Task is interrupted")
        end
      end
    else
      @logger.info("#{self.class}: Not enabled")
    end
  end

  def time
    Time.now.to_i
  end

end
