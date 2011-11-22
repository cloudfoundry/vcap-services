SERVICES_DIR = %w(atmos mongodb mysql neo4j postgresql rabbit redis service_broker)
AUX_DIR = %w(base)

desc "Run integration tests."
task "tests" do |t|
  system "cd tests; bundle exec rake tests"
end

namespace "bundler" do
  desc "bundler install"
  task "install" do
    (SERVICES_DIR + AUX_DIR).each do |dir|
      puts ">>>>>>>> enter #{dir}"
      system "cd #{dir} && bundle install"
    end
  end

  desc "Update base gem"
  task "update_base" do
    system "cd base && rake bundler:install"
    SERVICES_DIR.each do |dir|
      puts ">>>>>>>> enter #{dir}"
      system "cd #{dir} && rm -f vendor/cache/vcap_services_base-*.gem && bundle install"
    end
  end
end
