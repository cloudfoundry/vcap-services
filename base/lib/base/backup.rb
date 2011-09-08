require 'rubygems'
require 'bundler/setup'
require 'optparse'
require 'timeout'
require 'fileutils'
require 'logger'
require 'logging'
require 'yaml'
require 'pathname'

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..')
require 'vcap/logging'

$:.unshift File.dirname(__FILE__)
require 'abstract'


module VCAP
  module Services
    module Base
    end
  end
end

#@config_file Full path to config file
#@config Config hash for config file
#@logger
#@nfs_base NFS base path
class VCAP::Services::Base::Backup
  abstract :default_config_file
  abstract :backup_db

  def script_file
    $0
  end

  def single_app(&blk)
    if File.open(script_file).flock(File::LOCK_EX|File::LOCK_NB)
      blk.call
    else
      warn "Script #{ script_file } is already running"
    end
  end

  def start
    single_app do
      puts "#{File.basename(script_file)} starts"

      @config_file = default_config_file

      parse_options

      puts "Load config file"
      # load conf file
      begin
        @config = YAML.load(File.open(@config_file))
      rescue => e
        puts "Could not read configuration file: #{e}"
        exit
      end

      # Setup logger
      puts @config["logging"]
      VCAP::Logging.setup_from_config(@config["logging"])
      # Use running binary name for logger identity name.
      @logger = VCAP::Logging.logger(File.basename(script_file))

      puts "Check mount points"
      check_mount_points

      # make sure backup dir on nfs storage exists
      @nfs_base = @config["backup_base_dir"] + "/backups/" + @config["service_name"]
      puts "Check NFS base"
      if File.directory? @nfs_base
        echo @nfs_base + " exists"
      else
        echo @nfs_base + " does not exist, create it"
        begin
          FileUtils.mkdir_p @nfs_base
        rescue => e
          echo "Could not create dir on nfs!",true
          exit
        end
      end

      puts "Run backup task"
      backup_db
      puts "#{File.basename(script_file)} task is completed"

    end
  rescue => e
    puts "Error: #{e.message}\n #{e.backtrace}"
  end

  def get_dump_path(name,mode=0)
    name = name.sub(/^(mongodb|redis)-/,'')
    case mode
    when 1
      File.join(@config['backup_base_dir'], 'backups', @config['service_name'],name, Time.new.to_i.to_s,@config['node_id'])
    else
      File.join(@config['backup_base_dir'], 'backups', @config['service_name'], name[0,2], name[2,2], name[4,2], name, Time.new.to_i.to_s)
    end
  end

  def check_mount_points
    # make sure the backup base dir is mounted
    pn = Pathname.new(@config["backup_base_dir"])
    if !@tolerant && !pn.mountpoint?
      echo @config["backup_base_dir"] + " is not mounted, exit",true
      exit
    end
  end

  def echo(output, err=false)
    puts output
    if err
      @logger.error(output) unless @logger.nil?
    else
      @logger.info(output) unless @logger.nil?
    end
  end

  def parse_options
    OptionParser.new do |opts|
      opts.banner = "Usage: #{File.basename(script_file)} [options]"
      opts.on("-c", "--config [ARG]", "Node configuration File") do |opt|
        @config_file = opt
      end
      opts.on("-h", "--help", "Help") do
        puts opts
        exit
      end
      opts.on("-t", "--tolerant",    "Tolerant mode") do
        @tolerant = true
      end
      more_options(opts)
    end.parse!
  end

  def more_options(opts)
  end
end

class CMDHandle

  def initialize(cmd, timeout=nil, &blk)
    @cmd  = cmd
    @timeout = timeout
    @errback = blk
  end

  def run
    pid = fork
    if pid
      # parent process
      success = false
      begin
        success = Timeout::timeout(@timeout) do
          Process.waitpid(pid)
          value = $?.exitstatus
          @errback.call(@cmd, value, "No message.") if value != 0 && @errback
          return value == 0
        end
      rescue Timeout::Error
        Process.detach(pid)
        Process.kill("KILL", pid)
        @errback.call(@cmd, -1, "Killed due to timeout.") if @errback
        return false
      end
    else
      # child process
      exec(@cmd)
    end
  end

  def self.execute(cmd, timeout = nil, *args)
    errb = args.pop if args.last.is_a? Proc
    instance = self.new(cmd, timeout, &errb)
    instance.run
  end
end

