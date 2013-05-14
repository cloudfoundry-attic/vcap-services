require_relative 'component_runner'

class NatsRunner < ComponentRunner
  def start
    add_pid Process.spawn "bundle exec nats-server", log_options(:nats)
    wait_for_tcp_ready("NATS", 4222)
  end
end

