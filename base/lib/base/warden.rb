require "warden/client"
require "utils"

module VCAP::Services::Base::Warden

  def self.included(base)
    base.extend(ClassMethods)
  end

  module ClassMethods
    def warden_connect
      warden_client = Warden::Client.new("/tmp/warden.sock")
      warden_client.connect
      warden_client
    end
  end

  def loop_create(max_size)
    self.class.sh "dd if=/dev/null of=#{image_file} bs=1M seek=#{max_size}"
    self.class.sh "mkfs.ext4 -q -F -O \"^has_journal,uninit_bg\" #{image_file}"
  end

  def loop_setdown
    self.class.sh "umount #{base_dir}"
  end

  def loop_setup
    self.class.sh "mount -n -o loop #{image_file} #{base_dir}"
  end

  def loop_setup?
    mounted = false
    File.open("/proc/mounts", mode="r") do |f|
      f.each do |w|
        if Regexp.new(base_dir) =~ w
          mounted = true
          break
        end
      end
    end
    mounted
  end

  def container_start(cmd, bind_mounts=[])
    warden = self.class.warden_connect
    unless bind_mounts.empty?
      req = ["create", {"bind_mounts" => bind_mounts}]
    else
      req = ["create"]
    end
    handle = warden.call(req)
    req = ["info", handle]
    info = warden.call(req)
    ip = info["container_ip"]
    req = ["spawn", handle, cmd]
    warden.call(req)
    warden.disconnect
    sleep 1
    [handle, ip]
  end

  def container_stop(handle)
    warden = self.class.warden_connect
    req = ["stop", handle]
    warden.call(req)
    req = ["destroy", handle]
    warden.call(req)
    warden.disconnect
    true
  end

  def container_running?(handle)
    if handle == ''
      return false
    end

    begin
      warden = self.class.warden_connect
      req = ["info", handle]
      warden.call(req)
      return true
    rescue => e
      return false
    ensure
      warden.disconnect
    end
  end

  def map_port(src_port, dest_ip, dest_port)
    rule = [ "--protocol tcp",
             "--dport #{src_port}",
             "--jump DNAT",
             "--to-destination #{dest_ip}:#{dest_port}" ]
    self.class.sh "iptables -t nat -A PREROUTING #{rule.join(" ")}"
  end

  def unmap_port(src_port, dest_ip, dest_port)
    rule = [ "--protocol tcp",
             "--dport #{src_port}",
             "--jump DNAT",
             "--to-destination #{dest_ip}:#{dest_port}" ]
    self.class.sh "iptables -t nat -D PREROUTING #{rule.join(" ")}"
  end
end
