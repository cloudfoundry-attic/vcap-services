# Copyright (c) 2009-2011 VMware, Inc.
$LOAD_PATH.unshift File.dirname(__FILE__)

require 'rotator'

module VCAP
  module Services
    module Backup
    end
  end
end

class VCAP::Services::Backup::Manager < VCAP::Services::Base::Base

  def initialize(options)
    super(options)
    @logger = options[:logger]
    @once = options[:once]
    @logger.info("#{self.class}: Initializing")
    @wakeup_interval = options[:wakeup_interval]
    @mountpoint = options[:root]
    @root = File.join(options[:root], "backups")
    @tasks = [
      VCAP::Services::Backup::Rotator.new(self, options[:rotation])
    ]
    @enable = options[:enable]
    @shutdown = false
    @run_lock = Mutex.new

    z_interval = options[:z_interval]
    EM.add_periodic_timer(z_interval) do
      EM.defer { update_varz }
    end
    EM.add_timer(5) do
      EM.defer { update_varz }
    end
  end

  attr_reader :root
  attr_reader :logger

  def exit_fun
    @logger.info("Terminating the application")
    @shutdown = true
    EM.defer do
      @run_lock.synchronize { shutdown; EM.stop; }
    end
  end

  def shutdown?
    @shutdown
  end

  def start
    @logger.info("#{self.class}: Starting")
    trap("TERM"){ exit_fun }
    trap("INT"){ exit_fun }

    if @once
      run
    else
      EM.add_periodic_timer(@wakeup_interval) {run}
    end
  end

  def run
    Fiber.new do
      if @enable
        @run_lock.synchronize do
          begin
            @logger.info("#{self.class}: Running")
            @tasks.each do |task|
              @logger.warn("#{self.class}: #{task.class} failed") unless task.run
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
      exit_fun if @once
    end.resume
  end

  def time
    Time.now.to_i
  end

  def service_name
    "BackupManager"
  end
  alias_method :service_description, :service_name

  def flavor
    "Node"
  end

  def on_connect_node
  end

  def varz_details
    varz = {}
    dev, total, used, available, percentage, mountpoint = disk_report(@mountpoint)

    varz[:disk_total_size] = total
    varz[:disk_used_size] = used
    varz[:disk_available_size] = available
    varz[:disk_percentage] = percentage

    varz
  rescue => e
    @logger.error("Error during generate varz: #{e}")
    {}
  end

  def disk_report(path)
    `df -Pk #{path}|grep #{path}`.split(' ')
  end

end
