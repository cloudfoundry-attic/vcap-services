# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), ".")

require "filesystem_service/base_provisioner"

class VCAP::Services::Filesystem::NFSProvisioner < VCAP::Services::Filesystem::BaseProvisioner
  def all_instances_list
    instances_list = []
    @backends.each do |backend|
      dir = backend["mount"]
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
      host    = handle[:credentials]["internal"]["host"]
      export  = handle[:credentials]["internal"]["export"]
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

  def get_instance_dir(name, backend)
    File.join(backend["mount"], name)
  end

  def gen_credentials(name, backend)
    credentials = {
      "internal"  => {
        "fs_type" => @fs_type,
        "name"    => name,
        "host"    => backend["host"],
        "export"  => backend["export"],
      }
    }
  end
end
