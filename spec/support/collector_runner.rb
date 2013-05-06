require_relative 'component_runner'

class CollectorRunner < ComponentRunner
  # def set_reaction(blk)
    # @reaction_blk = blk
  # end
  attr_writer :reaction_blk

  def start_fake_tsdb
    add_thread Thread.new {
      Socket.tcp_server_loop(4242) do |s, client_addrinfo|
        begin
          puts "Listening to port 4242 for Collector (OpenTSDB) data..."
          while true
            data = s.readline
            if @reaction_blk
              @reaction_blk.call(data)
            else
              # print data
            end
          end
        rescue EOFError => e
          puts "Stream closed! #{e}"
        rescue => e
          p e
          puts e.backtrace.join("\n  ")
        ensure
          s.close
          #raise "Die, thread die! Now you should work"
          #puts s.inspect
        end
      end
    }
  end

  def checkout_collector
    Dir.chdir tmp_dir do
      FileUtils.mkdir_p "log"
      sh "git clone --recursive git://github.com/cloudfoundry/vcap-tools.git" unless Dir.exist?("vcap-tools")
      Dir.chdir "vcap-tools" do
        if ENV['NO_CHECKOUT'].nil? || ENV['NO_CHECKOUT'].empty?
          unless `git status -s`.empty?
            raise 'There are outstanding changes in collector. Need to set NO_CHECKOUT env'
          end
          sh "git fetch && git reset --hard origin/master && git submodule update --init"
        end

        Bundler.with_clean_env do
          Dir.chdir "collector" do
            sh "bundle install >> #{tmp_dir}/log/bundle.out"
          end
        end
      end
      $checked_out_collector = true
    end
  end

  def start
    thread = start_fake_tsdb
    checkout_collector unless $checked_out_collector
    Dir.chdir "#{tmp_dir}/vcap-tools/collector" do
      Bundler.with_clean_env do
        add_pid Process.spawn(
          {"CONFIG_FILE" => asset("collector.yml")},
          "bundle exec ./bin/collector", log_options(:collector)
        )
      end
    end
  end
end
