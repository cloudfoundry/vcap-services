# Copyright (c) 2009-2011 VMware, Inc.

module VCAP
  module Services
    module VBlob
      module Utils

        include VCAP::Services::Base::Warden

        VBLOB_TIMEOUT = 3

        def data_dir
          File.join(base_dir,'vblob_data')
        end

        def data_dir?
          Dir.exists?(data_dir)
        end

        def record_service_log(service_id)
          @logger.warn(" *** BEGIN vblob log - instance: #{service_id}")
          @logger.warn("")
          file = File.new(log_file_vblob, 'r')
          while (line = file.gets)
            @logger.warn(line.chomp!)
          end
        rescue => e
          @logger.warn(e)
        ensure
          @logger.warn(" *** END vblob log - instance: #{service_id}")
          @logger.warn("")
        end

        def close_fds
          3.upto(get_max_open_fd) do |fd|
            begin
              IO.for_fd(fd, "r").close
            rescue
            end
          end
        end

        def get_max_open_fd
          max = 0

          dir = nil
          if File.directory?("/proc/self/fd/") # Linux
            dir = "/proc/self/fd/"
          elsif File.directory?("/dev/fd/") # Mac
            dir = "/dev/fd/"
          end

          if dir
            Dir.foreach(dir) do |entry|
              begin
                pid = Integer(entry)
                max = pid if pid > max
              rescue
              end
            end
          else
            max = 65535
          end

          max
        end

      end
    end
  end
end
