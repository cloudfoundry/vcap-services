# Copyright (c) 2009-2011 VMware, Inc.
require 'optparse'
require 'uri'
require 'pp'
require 'erb'
require 'yaml'
require 'rubygems'
require 'bundler/setup'
require 'sinatra'
require 'eventmachine'
require 'em-http'
require 'nats/client'
require 'yajl'
require 'yajl/json_gem'
require 'net/http'
require 'uri'
require 'logger'
require 'fileutils'

class Discover
    
  def initialize
    begin
      @config = YAML.load(open_file("config.yml"))
    rescue => e
      puts "Could not read configuration file: #{e}"
      exit
    end
    
    use Rack::Auth::Basic do |username, password|
      [username, password] == [config['auth']['user'], config['auth']['pass']]
    end
    
    @orphan_instances = {
      :redis => {},
      :mongodb =>{},
      :mysql => {},
      :neo4j => {},
      :postgresql => {},
      :others => {}
    }
    @orphan_bindings = {
      :redis => {},
      :mongodb => {},
      :mysql => {},
      :neo4j => {},
      :postgresql => {},
      :others => {}
    }
  end
  
  def refresh
    @components = {}
    NATS.publish('vcap.component.discover', '', @inbox)
  end

  def run()

    # Hold onto the component and service node endpoints
    @components = {}
    @service_nodes = {}
    
    # Create the report if the report does not exist / fetch the file handle
    create_report()
    
    # Create the Logger file
    @logger = Logger.new(@config["logfile"])

    # Install error handler and NATS retry logic..
    EM.error_handler { |e|
      STDERR.puts("#{e.message} #{e.backtrace.join("\n")}")
      if e.kind_of? NATS::Error
        STDERR.puts "NATS problem, #{e}"
      end
    }
    NATS.start(:uri => @config['mbus']) do
    # Watch for new components
      NATS.subscribe('vcap.component.announce') do |msg|
        component_discover(msg)
      end
        
      # Keep endpoint around for subsequent pings
      @inbox = NATS.create_inbox
      NATS.subscribe(@inbox) { |msg| component_discover(msg) }
      
      # Ping for status/discovery immediately
      NATS.publish('vcap.component.discover', '', @inbox)
      
      # Now setup a periodic timer to keep discovering new components
      EM.add_periodic_timer(@config["refresh"]) { refresh }
    end
  end

  def component_discover(msg)
    info = Yajl::Parser.parse(msg, :symbolize_keys => true)
    if (!info[:type] || !info[:uuid] || !info[:host])
      STDERR.puts "Received non-comformant reply for component discover: #{msg}"
    else
      unless  @components[info[:uuid]]
        if (info[:type].end_with?("Provisioner")) 
          comp = info
          url = URI.parse("http://#{comp[:host]}/varz")
          json = nil
          Net::HTTP.start(url.host, url.port) { |http|
            req = Net::HTTP::Get.new('/varz')
            req.basic_auth comp[:credentials][0], comp[:credentials][1]
            response = http.request(req)
            varz = Yajl::Parser.parse(response.body, :symbolize_keys => true)
            varz_parser(varz)
          }
        end
      end
    end
  end
    
  def varz_parser(varz)
    dbname = case
    when varz[:type] == "RaaS-Provisioner" then :redis
    when varz[:type] == "MongoaaS-Provisioner" then :mongodb
    when varz[:type] == "MyaaS-Provisioner" then :mysql
    when varz[:type] == "Neo4jaaS-Provisioner" then :neo4j
    when varz[:type] == "AuaaS-Provisioner" then :postgresql
    else :other
    end
    
    instances = varz[:orphan_instances]
    bindings = varz[:orphan_bindings]
    
    # update the current orphan instance and binding cache
    @orphan_instances[:dbname] = instances
    @orphan_bindings[:dbname] = bindings
    
    # log the orphan details into one log file
    log_orphan(dbname, instances, bindings)
    
    # generate the report based on the orphan numbers
    generate_report(dbname, instances, bindings)
  end
  
  def log_orphan(dbname, orphan_instances, orphan_bindings)
    output = dbname.to_s
    
    # output the orphan instances
    output = output + ": orphan_instances=> "
    if orphan_instances.empty?
      output = output + "none"
    else
      orphan_instances.each_value do |oi|
        output = output.concat(oi.to_s) + ","
      end
      output[-2] = "]"
    end

    # output the orphan bindings
    output = output + "   orphan_bindings=> "
    if orphan_bindings.empty?
      output = output + "none"
    else
      orphan_bindings.each_value do |ob|
        output = output.concat(ob.to_s) + ","
      end
      output[-2] = "]"
    end

    @logger.info {output}
  end
  
  def generate_report(dbname, new_instances, new_bindings)
    # get the time now
    time = Time.new
    
    # calculate the size of orphan instances
    instances_count = 0
    new_instances.each_value do |ni|
      instances_count = instances_count + ni.size
    end
    
    # calculate the size of orphan bindings
    bindings_count = 0
    new_bindings.each_value do |nb|
      bindings_count = bindings_count + nb.size
    end
    
    # calculate new orphan instances (set difference)
    new_instances_count = count_orphan(new_instances, @orphan_instances[dbname])
    new_bindings_count = count_orphan(new_bindings, @orphan_bindings[dbname])
    fixed_instances_count = count_orphan(@orphan_instances[dbname], new_instances)
    fixed_bindings_count = count_orphan(@orphan_bindings[dbname], new_bindings)
    
    # Write the report
    report = File.open("reports/#{dbname}", "a")
    content = ""
    content = content + time.to_s + "|"
    content = content + bindings_count.to_s.ljust(17) + "|"
    content = content + instances_count.to_s.ljust(18) + "|"
    content = content + new_bindings_count.to_s.ljust(21) + "|"
    content = content + new_instances_count.to_s.ljust(22) + "|"
    content = content + fixed_instances_count.to_s.ljust(24) + "|"
    content = content + fixed_bindings_count.to_s.ljust(23) + "|\n"
    report.write(content)
    report.close
    
    # Update current record
    @orphan_bindings[dbname] = new_bindings
    @orphan_instances[dbname] = new_instances
    
  end
    
  def count_orphan(minu, subtra)
    # use set difference here
    if minu.empty?
      return 0
    else
      count = 0
      minu.each_key do |key|
        if ( subtra.empty? || (not subtra.has_key?(key)) )
          count = count + minu[key].size
        else
          count = count + (minu[key] - subtra[key]).size
        end
      end
      return count
    end
  end
  
  def create_report()
    # Check whether there exists a directory called report
    unless File::directory?("reports")
      FileUtils.mkdir("reports")
    end
    
    # Create the report for all the databases if it does not exist before
    create_orphan_report("redis")
    create_orphan_report("mongodb")
    create_orphan_report("mysql")
    create_orphan_report("neo4j")
    create_orphan_report("postgresql")
    create_orphan_report("others")
  end
  
  def create_orphan_report(dbname)
    
    # Create a new report
    # Need to remove the original report since the cached orphans are removed
    report = File.open("./reports/#{dbname}", "w")
    content = "-----------time----------|"
    content = content + "-orphan bindings-|"
    content = content + "-orphan instances-|"
    content = content + "-new orphan bindings-|"
    content = content + "-new orphan instances-|"
    content = content + "-fixed orphan instances-|"
    content = content + "-fixed orphan bindings-|"
    report.puts(content)
    
    report.close
  end
  
  def open_file(file_address)
    begin
      myfile = File.open(file_address)
      return myfile
    rescue => e
      STDERR.puts "Could not read the file: #{e}"
      exit
    end
  end
end

EM.next_tick do
  Discover.new.run()
end
