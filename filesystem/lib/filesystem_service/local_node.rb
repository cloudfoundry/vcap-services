# Copyright (c) 2009-2012 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), ".")
require "filesystem_service/base_node"

class VCAP::Services::Filesystem::LocalNode < VCAP::Services::Filesystem::BaseNode
  def get_backend(cred=nil)
    if cred
      local_path = cred["internal"]["local_path"]
      @backends.each do |backend|
        if backend["local_path"] == local_path
          return backend
        end
      end if local_path
      return nil
    else
      # Simple round-robin load-balancing; TODO: Something smarter?
      return nil if @backends == nil || @backends.empty?
      index = @backend_index
      @backend_index = (@backend_index + 1) % @backends.size
      return @backends[index]
    end
  end

  def get_instance_dir(name, backend)
    File.join(backend["local_path"], name)
  end

  def gen_credentials(name, backend)
    credentials = {
      "fs_type"     => @fs_type,
      "internal"    => {
        "name"        => name,
        "local_path"  => backend["local_path"]
      }
    }
  end
end
