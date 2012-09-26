# Copyright (c) 2009-2012 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require 'spec_helper'

describe 'Serialization data server - internal store' do

  before :each do
    @opts = load_config
    @opts[:serialization_base_dir] = "/tmp/spec_test_sds"
    @store = VCAP::Services::Serialization::Store.new(@opts)
    @redis = @store.connect_redis
    @service = "mysql"
    @service_id = "abcd12349999"
  end

  it "make file readable" do
    file = Tempfile.new('sds_store').path
    File.chmod(0600, file)
    old_permission = File.lstat(file).mode
    @store.make_file_world_readable(file)
    File.lstat(file).mode.should == old_permission | 0444
  end

  it "register and store a new file" do
    ori_file_path = Tempfile.new('sds_store').path
    code, file_token, path = @store.store_file(@service, @service_id, ori_file_path)
    code.to_i.should == 200
    File.exist?(path).should == true
    File.exist?(ori_file_path).should == false
    file_info_s = @store.get_file(@service, @service_id, file_token)
    file = nil
    time= nil
    begin
     file_info = JSON.parse(file_info_s)
     file = file_info["file"]
     time = file_info["time"]
    rescue
      file = nil
      time = nil
    end
    file.should == path
    time.should_not nil

    expire_file = @redis.lindex(@store.redis_upload_purge_queue, 0)
    begin
      e_file = JSON.parse(expire_file)
      e_file["time"].should == time
      e_file["token"].should == token
      e_file["file_path"].should == path
    rescue
    end
  end

  it "purge expired file" do
    new_opts = @opts.dup
    new_opts[:expire_time] = 2
    new_opts[:purge_num] = 3
    store = VCAP::Services::Serialization::Store.new(new_opts)
    redis = store.connect_redis
    files = []
    5.times do |i|
      ori_file_path = Tempfile.new('sds_store').path
      code, file_token, path = store.store_file(@service, @service_id, ori_file_path)
      code.should == 200
      redis.hget(store.redis_file_key(@service, @service_id), file_token).should_not nil
      files << [file_token, path]
    end
    redis.llen(store.redis_upload_purge_queue).should == 5
    sleep 3
    store.purge_expired
    redis.llen(store.redis_upload_purge_queue).should == 2
    index = 0
    files.each do |token, file|
      if index < 3
        redis.hget(store.redis_file_key(@servie, @servie_id), token).should == nil
        File.exist?(file).should == false
      else
        redis.hget(store.redis_file_key(@service, @service_id), token).should_not == nil
        File.exist?(file).should == true
      end
      index += 1
    end

  end

  it "try to unregister a file" do
    new_opts = @opts.dup
    new_opts[:expire_time] = 2
    store = VCAP::Services::Serialization::Store.new(new_opts)
    redis = store.connect_redis
    ori_file_path = Tempfile.new('sds_store').path
    code1, file_token1, path1 = store.store_file(@service, @service_id, ori_file_path)
    code1.should == 200
    ori_file_path = Tempfile.new('sds_store').path
    code2, file_token2, path2 = store.store_file(@service, @service_id, ori_file_path)
    code2.should == 200

    file, time = store.try_unregister_file(@service, @service_id, file_token1, false)
    file.should == path1
    file, time = store.try_unregister_file(@service, @service_id, file_token2, true)
    file.should == nil
    time.should == nil

    sleep 3

    file, time = store.try_unregister_file(@service, @service_id, false )
    file.should == nil
    time.should == nil
  end

  after :all do
    FileUtils.rm_rf("/tmp/sds_store.*")
    FileUtils.rm_rf("/tmp/spec_test_sds")
  end

end
