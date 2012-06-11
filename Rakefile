SERVICES_DIR = %w(atmos filesystem memcached mongodb mysql neo4j postgresql rabbit redis service_broker vblob tools/backup/manager)

desc "Run integration tests."
task "tests" do |t|
  system "cd tests; bundle exec rake tests"
end

namespace "bundler" do
  desc "Run bundle install in services components"
  task "batch_install" do
    SERVICES_DIR.each do |dir|
      puts ">>>>>>>> enter #{dir}"
      system "cd #{dir} && bundle install"
    end
  end
end
