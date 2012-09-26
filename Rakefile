require 'tmpdir'

SERVICES_DIR = %w(atmos couchdb echo elasticsearch filesystem memcached mongodb mysql neo4j postgresql rabbit redis service_broker vblob tools/backup/manager marketplace)

desc "Run integration tests."
task "tests" do |t|
  system "cd tests; bundle exec rake tests"
end

namespace "bundler" do
  def exec_in_svc_dir
    SERVICES_DIR.each do |dir|
      puts ">>>>>>>> enter #{dir}"
      Dir.chdir(dir) do
        yield dir
      end
    end
  end

  def prune_git(path, gem)
    out = ''
    IO.foreach(path) do |line|
      if line =~ /.*#{gem}.*/
        data = line.split(',')
        data.delete_if{ |item| item =~ /^\s*:(git|branch|tag|ref)/ }
        line = data.join(',')
        line << "\n"
      end
      out << line
    end

    open(path, 'w') { |f| f.write(out) }
  end

  # usage: rake bundler:update![oldref,newref]
  desc "Update git ref in Gemfile"
  task :update, :oref, :nref do |t, args|
    exec_in_svc_dir { |_| sh "sed -i \"s/#{args[:oref]}/#{args[:nref]}/g\" Gemfile && bundle install" }
  end

  desc "Dry run update"
  task :update_dry, :oref, :nref do |t, args|
    exec_in_svc_dir { |_| sh "sed \"s/#{args[:oref]}/#{args[:nref]}/g\" Gemfile" }
  end

  # usage: rake bundler:gerrit_vendor[gem_name,'<repo>','<refspec>']
  desc "Change the gem source from git reference to local vendor"
  task :gerrit_vendor, :gem_name, :repo, :refspec do |t, args|
    gem_name = args[:gem_name]
    repo = args[:repo]
    refspec = args[:refspec]

    working_dir = Dir.mktmpdir
    `git clone #{repo} #{working_dir}`

    def exec_in_gem_dir(base_dir, gname)
      Dir.chdir(base_dir) do
        if File.exist? "#{gname}.gemspec"
          yield if block_given?
        else
          if File.directory? gname
            Dir.chdir(gname) { yield if block_given? }
          else
            abort
          end
        end
      end
    end

    exec_in_gem_dir(working_dir, gem_name) do
      abort unless system "git fetch #{repo} #{refspec} && git checkout FETCH_HEAD && gem build #{gem_name}.gemspec && gem install #{gem_name}*.gem"
    end

    exec_in_svc_dir do |dir|
      prune_git('Gemfile', gem_name)
      sh "rm -f vendor/cache/#{gem_name}*.gem && bundle install"
    end

    FileUtils.rm_rf(working_dir)
  end
end
