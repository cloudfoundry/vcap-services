# Copyright (c) 2009-2011 VMware, Inc.
require "yaml"
require "net/ssh"
require "pty"
require "expect"
require "pg"
require "yajl"

class ToolsTest

  def initialize
    @config = YAML.load_file(File.expand_path("../../config/tools_test.yml", __FILE__))
    @backup_manager = @config["backup_manager"]
    @gateway = nil
    @instance_alias = nil
    @credentials = nil
    @binding_credentials = nil
    @instance_alias_array = []
    @credentials_hash = {}
    @backup_path = nil
    @gateway_uri = nil
  end

  def start
    prepare
    work
    verify
    clean
  end

  def prepare
    puts "1. Preparing..."
    Net::SSH.start(@gateway, "root", :password => "ca\$hc0w") do |ssh|
      output = ssh.exec!("netstat -lnp | grep ruby | tail -n 3 | awk -F' ' '{print $4}'")
      output.split(/\n/).each do |url|
        if %x[curl #{url} 2>/dev/null].index("Invalid Content-Type")
          @gateway_uri = url
        end
      end
    end
  end

  def work
    puts "2. Working..."
  end

  def verify
    puts "3. Verifying..."
  end

  def clean
    puts "4. Cleaning..."
    %x[vmc services].split(/\n/).each do |line|
      items = line.split(/ +/)
      if items.size == 5
        %x[vmc delete-service #{items[1]}]
      end
    end
    %x[vmc apps].split(/\n/).each do |line|
      items = line.split(/ +/)
      if items.size == 10
        %x[vmc delete #{items[1]}]
      end
    end
  end

  def update_credentials
    conn = PGconn.open(:host => @config["ccdb"]["host"], :port => @config["ccdb"]["port"], :dbname => @config["ccdb"]["database"], :user => @config["ccdb"]["username"], :password => @config["ccdb"]["password"])
    @instance_alias_array.each do |instance_alias|
      res = conn.exec("select * from service_configs where alias = '#{instance_alias}'")
      @credentials_hash[instance_alias] = YAML.load(res[0]["credentials"])
    end
    if @instance_alias == nil
      @instance_alias = @instance_alias_array[0]
    end
    res = conn.exec("select * from service_configs where alias = '#{@instance_alias}'")
    @credentials = YAML.load(res[0]["credentials"])
  end

  def add_app(name)
    PTY.spawn("vmc push #{name}") do |reader, writer, pid|
      $expect_verbose = true

      reader.expect(/^Would you like to deploy from the current directory.*/) { writer.puts("n") }
      reader.expect(/^Please enter in the deployment path.*/) { writer.puts("./apps/#{name}") }
      reader.expect(/^Application Deployed URL.*/) { writer.puts }
      reader.expect(/^Detected a Sinatra Application, is this correct.*/) { writer.puts }
      reader.expect(/^Memory Reservation.*/) { writer.puts }
      reader.expect(/^Would you like to bind any services.*/) { writer.puts }
      reader.expect(/^Starting Application:.*/) { puts reader.gets }
    end
  end

  def delete_app(name)
    %x[vmc delete #{name}]
  end

  def get_app_credentials(url)
    output = %x[curl #{url} 2>/dev/null]
    res = Yajl::Parser.parse(output.split(/[\[\]]/)[1])
    res["options"]
  end

  def add_redis_data(credentials, key, value)
    redis = Redis.new({:host => credentials[("hostname")], :port => credentials["port"], :password => credentials["password"]})
    redis.set(key, value)
    redis.quit
  end

  def backup_redis_node(credentials)
    Net::SSH.start(credentials["hostname"], "root", :password => "ca\$hc0w") do |ssh|
      output = ssh.exec!("/var/vcap/packages/ruby/bin/ruby /var/vcap/packages/redis_node/services/redis/bin/redis_backup -c /var/vcap/jobs/redis_node/config/redis_backup.yml")
      output.split(/\n/).each do |line|
        a = line.split(/: /)
        if a.size == 2 and a[0] == "new dir" and a[1].index(credentials["name"])
          @backup_path = a[1]
        end
      end
    end
  end

  def verify_redis_credentials(credentials, key=nil, value=nil)
    redis = Redis.new({:host => credentials["hostname"], :port => credentials["port"], :password => credentials["password"]})
    if key and redis.get(key) != value
      return false
    end
    redis.quit
    true
  rescue => e
    puts e.backtrace
    false
  end

  def add_mongodb_data(credentials, key, value)
    conn = Mongo::Connection.new(credentials["hostname"], credentials["port"])
    auth = conn.db(credentials["db"]).authenticate(credentials["username"], credentials["password"])
    coll = conn.db(credentials["db"]).collection("mongodb_test")
    coll.insert({"#{key}" => value})
    conn.close
  end

  def backup_mongodb_node(credentials)
    Net::SSH.start(credentials["hostname"], "root", :password => "ca\$hc0w") do |ssh|
      output = ssh.exec!("/var/vcap/packages/ruby/bin/ruby /var/vcap/packages/mongodb_node/services/mongodb/bin/mongodb_backup -c /var/vcap/jobs/mongodb_node/config/mongodb_backup.yml")
      output.split(/\n/).each do |line|
        a = line.split(/ /)
        if a[0] == "find"
          @backup_path = a[1]
        end
      end
    end
  end

  def verify_mongodb_credentials(credentials, key, value)
    conn = Mongo::Connection.new(credentials["hostname"], credentials["port"])
    auth = conn.db(credentials["db"]).authenticate(credentials["username"], credentials["password"])
    coll = conn.db(credentials["db"]).collection("mongodb_test")
    if coll.find().to_a[0][key] != value
      return false
    end
    conn.close
    true
  rescue => e
    puts e.backtrace
    false
  end

  def add_mysql_data(credentials, value, need_create_table=true)
    conn = Mysql.real_connect(credentials["hostname"], credentials["username"], credentials["password"], credentials["name"], credentials["port"])
    if need_create_table
      conn.query("create table test(id int)")
    end
    conn.query("insert into test value(#{value})")
    conn.close
  end

  def backup_mysql_node(credentials)
    Net::SSH.start(credentials["hostname"], "root", :password => "ca\$hc0w") do |ssh|
      ssh.exec!("/var/vcap/packages/ruby/bin/ruby /var/vcap/packages/mysql_node/services/mysql/bin/mysql_backup -c /var/vcap/jobs/mysql_node/config/mysql_backup.yml")
      dir = File.join('/mnt/backups/mysql', credentials["name"][0,2], credentials["name"][2,2], credentials["name"][4,2], credentials["name"])
      output = ssh.exec!("ls #{dir}")
      @backup_path = File.join(dir, output.split(/\n/).sort[-1])
    end
  end

  def verify_mysql_credentials(credentials, value)
    conn = Mysql.real_connect(credentials["hostname"], credentials["username"], credentials["password"], credentials["name"], credentials["port"])
    result = conn.query("select * from test")
    result.each do |row|
      if row[0] == value.to_s
        conn.close
        return true
      end
    end
    conn.close
    false
  rescue => e
    puts e.backtrace
    false
  end

  def restore
    Net::SSH.start(@backup_manager, "root", :password => "ca\$hc0w") do |ssh|
      ssh.exec!("/var/vcap/packages/ruby/bin/ruby /var/vcap/packages/backup_manager/services/tools/restore/bin/restore -u #{@gateway_uri} -b #{@backup_path}")
    end
  end

  def recover
    Net::SSH.start(@backup_manager, "root", :password => "ca\$hc0w") do |ssh|
      ssh.exec!("/var/vcap/packages/ruby/bin/ruby /var/vcap/packages/backup_manager/services/tools/restore/bin/restore -u #{@gateway_uri} -b #{@backup_path} -m recover")
    end
  end

  def rebalance(service_name)
    src_node_id = @credentials["node_id"]
    dst_node_id = @credentials_hash[@instance_alias_array[1]]["node_id"]
    Net::SSH.start(@backup_manager, "root", :password => "ca\$hc0w") do |ssh|
      puts "/var/vcap/packages/ruby/bin/ruby /var/vcap/packages/backup_manager/services/tools/rebalance/bin/rebalance -n #{@config["mbus"]} -s RaaS -p 100 #{src_node_id} #{dst_node_id}"
      puts ssh.exec!("/var/vcap/packages/ruby/bin/ruby /var/vcap/packages/backup_manager/services/tools/rebalance/bin/rebalance -n #{@config["mbus"]} -s #{service_name} -p 100 #{src_node_id} #{dst_node_id}")
    end
  end

end
