# Copyright (c) 2009-2011 VMware, Inc.
module VCAP
  module Services
    module Postgresql

      # Give various helper functions
      module Util
        VALID_CREDENTIAL_CHARACTERS = ("A".."Z").to_a + ("a".."z").to_a + ("0".."9").to_a

        def parse_property(hash, key, type, options = {})
          obj = hash[key]
          if obj.nil?
            raise "Missing required option: #{key}" unless options[:optional]
            nil
          elsif type == Range
            raise "Invalid Range object: #{obj}" unless obj.kind_of?(Hash)
            first, last = obj["first"], obj["last"]
            raise "Invalid Range object: #{obj}" unless first.kind_of?(Integer) and last.kind_of?(Integer)
            Range.new(first, last)
          else
            raise "Invalid #{type} object: #{obj}" unless obj.kind_of?(type)
            obj
          end
        end

        def create_logger(logdev, rotation, level)
          if String === logdev
            dir = File.dirname(logdev)
            FileUtils.mkdir_p(dir) unless File.directory?(dir)
          end
          logger = Logger.new(logdev, rotation)
          logger.level = case level
            when "DEBUG" then Logger::DEBUG
            when "INFO" then Logger::INFO
            when "WARN" then Logger::WARN
            when "ERROR" then Logger::ERROR
            when "FATAL" then Logger::FATAL
            else Logger::UNKNOWN
          end
          logger
        end

        def generate_credential(length=12)
          Array.new(length) { VALID_CREDENTIAL_CHARACTERS[rand(VALID_CREDENTIAL_CHARACTERS.length)] }.join
        end

        # Return the version of postgresql
        def pg_version(conn)
          return -1 unless conn
          version= conn.query("select version()")
          reg = /([0~9.]{5})/
          return version[0]['version'].scan(reg)[0][0][0]
        end

        # Use this method for backuping and snapshoting the database
        # name: name of database
        # host: ip/hostname of your database node
        # port: port of your database listening
        # user
        # passwd
        # dump_file: the file to store the dumped data
        # opts: optional arguments
        #   dump_bin
        #   logger
        def dump_database(name, host, port, user, passwd, dump_file, opts = {} )
          raise "You must provide the following arguments: name, host, port, user, passwd, dump_file" unless name && host && port && user && passwd && dump_file

          dump_bin = opts[:dump_bin] || "pg_dump"
          #env PGPASSWORD=#{passwd}
          dump_cmd = "#{dump_bin} -Fc --host=#{host} --port=#{port} --username=#{user} --file=#{dump_file} #{name}"

          # running the command
          on_err = Proc.new do |cmd, code, msg|
            opts[:logger].error("CMD '#{cmd}' exit with code: #{code} & Message: #{msg}") if opts[:logger]  && opts[:logger].respond_to?(error)
          end

          result = CMDHandle.execute(dump_cmd, nil, on_err )
          raise "Failed to dump database of #{name}" unless result
          result
        end

        # Use this method for restoring and importing the database
        # name: name of database
        # host: ip/hostname of your database node
        # port: port of your database listening
        # user
        # passwd
        # dump_file: the file which stores the dumped data
        # opts: optional arguments
        #   restore_bin
        #   logger
        def restore_database(name, host, port, user, passwd, dump_file, opts = {} )
          raise "You must provide the following arguments: name, host, port, user, passwd, dump_file" unless name && host && port && user && passwd && dump_file

          restore_bin = opts[:restore_bin] || pg_restore
          #env PGPASSWORD=#{passwd} #
          restore_cmd = "{restore_bin} -h #{host} -p #{port} -U #{user} -d #{name} #{dump_file} "

          # running the command
          on_err = Proc.new do |cmd, code, msg|
            opts[:logger].error("CMD '#{cmd}' exit with code: #{code} & Message: #{msg}") if opts[:logger] && opts[:logger].respond_to?(error)
          end

          result = CMDHandle.execute(restore_cmd, nil, on_err )
          raise "Failed to restore database of #{name}" unless result
          result
        end

      end
    end
  end
end
