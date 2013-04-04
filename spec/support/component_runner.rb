require_relative 'ccng_client'

class ComponentRunner < Struct.new(:tmp_dir)
  include CcngClient

  def start
    raise NotImplementedError
  end

  def stop
    pids.each do |pid|
      Process.kill "TERM", pid if pid
    end
  end

  def pids
    @pids ||= []
  end

  def add_pid(pid)
    pids << pid
  end

  def log_options(name)
    FileUtils.mkdir_p("#{tmp_dir}/log")
    {
      :out => "#{tmp_dir}/log/#{name}.out",
      :err => "#{tmp_dir}/log/#{name}.err"
    }
  end

  def asset(file_name)
    File.expand_path(File.join(SPEC_ROOT, 'assets', file_name))
  end

  def wait_for_http_ready(label, port)
    print "Waiting for #{label}..."
    retries = 30
    begin
      response = client.get("http://localhost:#{port}/info")
      raise "Failed to connect, status: #{response.status}" unless response.ok?
      puts "ready!"
    rescue
      print "."
      sleep 0.3
      retries -= 1
      if retries > 0
        retry
      else
        puts
        raise
      end
    end
  end

  def wait_for_tcp_ready(label, port)
    print "Waiting for #{label}..."
    retries = 30
    begin
      sock = TCPSocket.new("localhost", port)
      sock.close
      puts "ready!"
    rescue
      print "."
      sleep 0.3
      retries -= 1
      if retries > 0
        retry
      else
        puts
        raise
      end
    end
  end

  def create_service_auth_token(label, service_token)
    ccng_post("/v2/service_auth_tokens",
              {label: label, provider:'core', token: service_token}
             )
  end

  def sh(cmd)
    raise "Unable to run #{cmd} in #{Dir.pwd}" unless system(cmd)
  end

  def user_guid
    12345
  end

  def client
    HTTPClient.new
  end
end
