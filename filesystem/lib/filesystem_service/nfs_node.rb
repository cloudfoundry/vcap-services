# Copyright (c) 2009-2012 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), ".")
require "filesystem_service/base_node"

class VCAP::Services::Filesystem::NFSNode < VCAP::Services::Filesystem::BaseNode
  def get_backend(cred=nil)
    if cred
      host    = cred["internal"]["host"]
      export  = cred["internal"]["export"]
      @backends.each do |backend|
        if backend["host"] == host && backend["export"] == export
          return backend
        end
      end if host && export
      return nil
    else
      # Simple round-robin load-balancing; TODO: Something smarter?
      return nil if @backends == nil || @backends.empty?
      index = @backend_index
      @backend_index = (@backend_index + 1) % @backends.size
      return @backends[index]
    end
  end

  def gen_credentials(name, backend)
    credentials = {
      "fs_type"   => @fs_type,
      "name"      => name,
      "internal"  => {
        "host"    => backend["host"],
        "export"  => backend["export"],
      }
    }
  end

  def get_instance_dir(name, backend)
    File.join(backend["mount"], name)
  end
end
