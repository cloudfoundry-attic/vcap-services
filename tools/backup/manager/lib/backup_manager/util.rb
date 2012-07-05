# Copyright (c) 2009-2011 VMware, Inc.
require 'time'
require 'em-http'
require 'json'
require 'json_message'
require 'services/api'
require 'fiber'

module VCAP
  module Services
    module Backup
    end
  end
end

module VCAP::Services::Backup::Util

  REQ_HEADER = { :head =>
    {
      'Content-Type'         => 'application/json',
      'X-VCAP-Service-Token' => '0xdeadbeef',
    }
  }

  ONE_DAY = 24*60*60

  def request_service_ins_fibered(uri)
    f = Fiber.current
    http = EM::HttpRequest.new(uri).get(REQ_HEADER)
    http.errback  { f.resume([false, http]) }
    http.callback { f.resume([true,  http]) }
    Fiber.yield
  end

  def rmdashr(path)
    if File.directory?(path)
      Dir.entries(path).each { |child|
        rmdashr(File.join(path, child)) unless dotty(child)
      }
      Dir.delete(path)
    else
      File.delete(path)
    end
  end

  def each_subdirectory(directory, &blk)
    if blk
      Dir.foreach(directory) { |child|
        unless dotty(child)
          path = File.join(directory, child)
          blk.call(path) if File.directory?(path)
        end
      }
    end
  end

  def dotty(s)
    s == '.' || s == '..'
  end

  def empty(path)
    Dir.entries(path).length < 3 # . & ..
  end

  def parent(path)
    File.absolute_path('..', path)
  end

end
