require "fileutils"
require "datamapper"

LOCK_FILE = "/var/vcap/sys/run/LOCK"

# Initialize lockfile
FileUtils.mkdir_p(File.dirname(LOCK_FILE))
File.open(LOCK_FILE, 'w') do |file|
  file.truncate(0)
end

module DataMapper

  class GlobalMutex
    def initialize(lockfile)
      @lockfile = lockfile
      @mutex = Mutex.new
    end

    def synchronize
      File.open(@lockfile, 'r') do |file|
        # step 1: Lock out all other processes
        file.flock(File::LOCK_EX)
        begin
          # step 2: Lock out all other threads within my process
          @mutex.synchronize do
            yield
          end
        ensure
          file.flock(File::LOCK_UN)
        end
      end
    end
  end

  MUTEX = GlobalMutex.new(LOCK_FILE)

  module Resource
    alias original_save save
    alias original_destroy destroy

    def save
      MUTEX.synchronize do
        original_save
      end
    end

    def destroy
      MUTEX.synchronize do
        original_destroy
      end
    end
  end

  module Model
    alias original_get get
    alias original_all all

    def get(*args)
      MUTEX.synchronize do
        original_get(*args)
      end
    end

    def all(*args)
      MUTEX.synchronize do
        original_all(*args)
      end
    end
  end

  # For auto_upgrade!
  module Migrations
    module SingletonMethods
      alias original_repository_execute repository_execute

      def repository_execute(*args)
        MUTEX.synchronize do
          original_repository_execute(*args)
        end
      end
    end
  end

end
