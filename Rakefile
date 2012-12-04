require 'tmpdir'

NG_SERVICES_DIR = %w(
  atmos
  couchdb
  echo
  elasticsearch
  filesystem
  marketplace
  memcached
  neo4j
  oauth2
  service_broker
  tools/backup/manager
  ng/mysql
  ng/postgresql
  ng/vblob
  ng/mongodb
  ng/redis
  ng/rabbit
  ng/memcached
)

NON_NG_SERVICE_DIR = %w(
  mongodb
  mysql
  postgresql
  rabbit
  redis
  vblob
)

desc "Run integration tests."
task "tests" do |t|
  system "cd tests; bundle exec rake tests"
end

namespace "bundler" do
  def exec_in_svc_dir(dirs, pattern=nil)
    dirs.each do |dir|
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

  def dirs_to_run(catalog, pattern)
    dirs = nil
    case catalog
    when "ng"
      dirs = NG_SERVICES_DIR
    when "nonng"
      dirs = NON_NG_SERVICE_DIR
    else
      dirs = NG_SERVICES_DIR + NON_NG_SERVICE_DIR
    end
    dirs.select {|d| d =~ /#{pattern}/}
  end

  # usage: rake bundler:update[oldref,newref,catalog,pattern]
  # for example, to update refs from '1234' to '2345' for all ng services
  # rake bundler:update[1234,2345,ng]
  # catalog & pattern are optional, if not provided, update all dirs
  desc "Update git ref in Gemfile"
  task :update, :oref, :nref, :catalog, :pattern do |t, args|
    dirs = dirs_to_run(args[:catalog], args[:pattern])
    exec_in_svc_dir(dirs) { |_| sh "sed -i \"s/#{args[:oref]}/#{args[:nref]}/g\" Gemfile && bundle install" }
  end

  desc "Dry run update"
  task :update_dry, :oref, :nref, :catalog, :pattern do |t, args|
    dirs = dirs_to_run(args[:catalog], args[:pattern])
    exec_in_svc_dir(dirs) { |_| sh "sed \"s/#{args[:oref]}/#{args[:nref]}/g\" Gemfile" }
  end

  # usage: rake bundler:gerrit_vendor[gem_name,'<repo>','<refspec>',catalog,pattern]
  desc "Change the gem source from git reference to local vendor"
  task :gerrit_vendor, :gem_name, :repo, :refspec, :catalog, :pattern do |t, args|
    gem_name = args[:gem_name]
    repo = args[:repo]
    refspec = args[:refspec]
    dirs = dirs_to_run(args[:catalog], args[:pattern])

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

    exec_in_svc_dir(dirs) do |dir|
      prune_git('Gemfile', gem_name)
      sh "rm -f vendor/cache/#{gem_name}*.gem && bundle install"
    end

    FileUtils.rm_rf(working_dir)
  end
end
