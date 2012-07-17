$:.unshift File.join(File.dirname(__FILE__), "..")
#require 'spec_helper'
require 'spec_helper'

MTDIR   = "/tmp/mtdir"
ROOTDIR = "/tmp/rootdir"

module DPXFS
end

describe DPXFS do

  before :all do
  end

  after :all do
  end

  describe "touch" do
    it "should support touch" do
      FileUtils.touch("testfile")
      File.exist?("testfile").should be_true
      FileUtils.rm_f("testfile")
      Dir.chdir(MTDIR) do
        begin
          FileUtils.touch("a" * 300)
        rescue => e
          e.is_a?(Errno::ENAMETOOLONG).should be_true
        end
      end
    end
  end

  describe "mkdir" do
    it "should support mkdir" do
      Dir.chdir(MTDIR) do
        FileUtils.mkdir_p("testdir", :mode => 0700)
        File.stat("testdir").mode.should == 040700
        FileUtils.rm_rf("testdir")
      end
    end
  end

  describe "read & write" do
    it "should be able to read/write files" do
      Dir.chdir(MTDIR) do
        file    = uuid
        content = ''
        10.times { content << uuid }
        open(file, 'w+') { |f| f.write(content) }
        IO.read(file).should == content
        FileUtils.rm_f(file)
      end
    end
  end

  describe "remove" do
    it "should be able to remove files" do
      Dir.chdir(MTDIR) do
        path = uuid
        create_file(path, "zero")
        File.exist?(path).should be_true
        FileUtils.rm_f(path)
        File.exist?(path).should be_false
      end
    end

    it "should be able to remove directory" do
      Dir.chdir(MTDIR) do
        path = uuid
        create_dir(path)
        File.exist?(path).should be_true
        File.directory?(path).should be_true
        FileUtils.rm_rf(path)
        File.exist?(path).should be_false
      end
    end
  end

  describe "copy" do
    it "should be able to copy files" do
      Dir.chdir(MTDIR) do
        old_path = File.join(MTDIR, uuid)
        new_path = File.join(MTDIR, uuid)
        create_file(old_path, "urandom")
        FileUtils.cp(old_path, new_path)
        old_md5 = hash_files([old_path])
        new_md5 = hash_files([new_path])
        FileUtils.rm_f(old_path)
        FileUtils.rm_f(new_path)
        old_md5.should == new_md5
      end
    end

    it "should be able to copy directory" do
      Dir.chdir(MTDIR) do
        old_dir = uuid
        new_dir = uuid
        create_dir(old_dir)
        FileUtils.cp_r(old_dir, new_dir)
        old_md5 = hash_files(Dir["#{old_dir}/*"])
        new_md5 = hash_files(Dir["#{new_dir}/*"])
        FileUtils.rm_rf(old_dir)
        FileUtils.rm_rf(new_dir)
        old_md5.should == new_md5
      end
    end
  end
end
