require 'fileutils'
require 'uuidtools'
require 'digest/md5'

def create_file(path, dev, count=16)
  `dd if=/dev/#{dev} of=#{path} bs=1M count=#{count} >/dev/null 2>&1`
end

def create_dir(path, file_count=10)
  FileUtils.mkdir_p(path)
  Dir.chdir(path) do
    file_count.times { create_file(uuid, "urandom", 1) }
  end
end

def hash_files(files)
  content = files.map{|f| File.read(f)}.join
  Digest::MD5.hexdigest(content).to_s
end

def uuid
  UUIDTools::UUID.random_create.to_s
end

