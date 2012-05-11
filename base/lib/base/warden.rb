# Copyright (c) 2009-2011 VMware, Inc.
require "warden/client"
require "utils"

module VCAP::Services::Base::Warden
  @@base_dir = ''
  @@log_dir = ''
  @@image_dir = ''
  @@max_db_size = 128

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
    self.class.sh "umount #{base_dir}", :raise => false if File.exist?(base_dir)
    if Dir.exist?(base_dir)
      logger.warn("Service #{self[:name]} base_dir:#{base_dir} already exists, deleting it")
      FileUtils.rm_rf(base_dir)
    end
    if Dir.exist?(log_dir)
      logger.warn("Service #{self[:name]} log_dir:#{log_dir} already exists, deleting it")
      FileUtils.rm_rf(log_dir)
    end
    if File.exist?(image_file)
      logger.warn("Service #{self[:name]} image_file:#{image_file} already exists, deleting it")
      FileUtils.rm_f(image_file)
    end
    FileUtils.mkdir_p(base_dir)
    FileUtils.mkdir_p(log_dir)
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

  def to_loopfile
    self.class.sh "mv #{base_dir} #{base_dir+"_bak"}"
    self.class.sh "mkdir -p #{base_dir}"
    self.class.sh "A=`du -sm #{base_dir+"_bak"} | awk '{ print $1 }'`;A=$((A+32));if [ $A -lt #{@@max_db_size} ]; then A=#{@@max_db_size}; fi;dd if=/dev/null of=#{image_file} bs=1M seek=$A"
    self.class.sh "mkfs.ext4 -q -F -O \"^has_journal,uninit_bg\" #{image_file}"
    self.class.sh "mount -n -o loop #{image_file} #{base_dir}"
    self.class.sh "cp -af #{base_dir+"_bak"}/* #{base_dir}", :timeout => 15.0
  end

  def migration_check
    if image_file?
      unless loop_setup?
        # for case where VM rebooted
        logger.info("Service #{self[:name]} mounting data file")
        loop_setup
      end
    else
      logger.warn("Service #{self[:name]} need migration to quota")
      to_loopfile
    end
  end

  # instance operation helper
  def delete
    # stop container
    stop if running?
    # delete log and service directory
    loop_setdown
    FileUtils.rm_rf(image_file)
    FileUtils.rm_rf(base_dir)
    FileUtils.rm_rf(log_dir)
    # delete recorder
    destroy!
  end

  def run
    self[:container], self[:ip] = container_start(service_script, [[base_dir, "/store/instance", {"mode" => "rw"}],
                                                                   [log_dir, "/store/log", {"mode" => "rw"}]])
    save!
    map_port(self[:port], self[:ip], service_port)
    true
  end

  def running?
    container_running?(self[:container])
  end

  def stop
    unmap_port(self[:port], self[:ip], service_port)
    container_stop(self[:container])
    self[:container] = ''
    save
    true
  end

  # warden container operation helper
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

  # port map helper
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

  # directory helper
  def image_file
    File.join(@@image_dir, "#{self[:name]}.img")
  end

  def base_dir
    File.join(@@base_dir, self[:name])
  end

  def log_dir
    File.join(@@log_dir, self[:name])
  end

  def image_file?
    File.exists?(image_file)
  end

  def base_dir?
    Dir.exists?(base_dir)
  end

  def log_dir?
    Dir.exists?(log_dir)
  end
end
