# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), ".")

require "filesystem_service/base_provisioner"

class VCAP::Services::Filesystem::LocalProvisioner < VCAP::Services::Filesystem::BaseProvisioner

  def all_instances_list
    instances_list = []
    @backends.each do |backend|
      dir = backend["local_path"]
      Dir.foreach(dir) do |child|
        unless child == "." || child ==".."
          instances_list << child if File.directory?(File.join(dir, child))
        end
      end
    end
    instances_list
  end

  def get_backend(handle=nil)
    if handle
      local_path = handle[:credentials]["internal"]["local_path"]
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
      "internal"  => {
        "fs_type"     => @fs_type,
        "name"        => name,
        "local_path"  => backend["local_path"]
      }
    }
  end
end
