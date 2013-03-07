#!/usr/bin/env ruby
require 'rubygems'
require 'aws-sdk'

def download_and_tar(source, destination=source, bucket)
  puts "Downloading tarball from S3..."
  obj = bucket.objects[source]
  File.open(destination, 'w') do |file|
    obj.read do |chunk|
      file.write(chunk)
    end
  end
  puts "Finished downloading tarball"
end

s3 = AWS::S3.new
bucket = s3.buckets['vcap-services-binaries']
download_and_tar(ARGV[0], bucket)
