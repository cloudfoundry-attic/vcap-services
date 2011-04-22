$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')
$:.unshift File.join(File.dirname(__FILE__), 'lib')
$LOAD_PATH.unshift(File.expand_path("../../../base/lib", __FILE__))

require "spec_helper"
require "atmos_service/atmos_provisioner"
require "atmos_service/atmos_helper"
require "uuidtools"

include VCAP::Services::Atmos

describe VCAP::Services::Atmos::Provisioner do

  before :all do
    puts "let's start :)"
    logger = Logger.new(STDOUT, "daily")
    @atmos_helper = Helper.new(logger)
  end

  it "should successfully new VCAP::Services::Atmos::Provisioner instance" do
    EM.run do
      @config = get_provisioner_config
      puts @config
      @sg = Provisioner.new(@config)
      puts @sg
      @sg.should_not be_nil
      EM.stop
    end
  end

  describe "provision_bind_unbind" do
    before :all do
      @raw_conf = get_raw_config()
      puts @raw_conf
      @subtenant_name_p = UUIDTools::UUID.random_create.to_s
      @subtenant_name_p1 = UUIDTools::UUID.random_create.to_s
      @token = UUIDTools::UUID.random_create.to_s
    end

    it "should successfully create atmos subtenant" do
      subtenant_id = @atmos_helper.createSubtenant(@subtenant_name_p)
      subtenant_id.should_not be_nil
    end
   
    it "should successfully create token under a subtenant" do
      shared_secret = @atmos_helper.createUser(@token, @subtenant_name_p)
      puts "token: " + @token + ", shared_secret: " + shared_secret 
      shared_secret.should_not be_nil
    end

    it "should successfully delete token under a subtenant" do
      success = @atmos_helper.deleteUser(@token, @subtenant_name_p)
      success.should == true
    end

    # create object on atmos through local temp file, then read it from atmos, then check it
    it "should successfully create object after bind" do
      subtenant_id = @atmos_helper.createSubtenant(@subtenant_name_p1)
      subtenant_id.should_not be_nil
      shared_secret = @atmos_helper.createUser(@token, @subtenant_name_p1)
      puts "token: " + @token + ", shared_secret: " + shared_secret
      shared_secret.should_not be_nil

      host = @raw_conf[:atmos][:host]
      puts "createobject, host: " + host
      remote_file_name = "etchosts"
      local_file_name = "/etc/hosts"
      local_temp_file_name = "/tmp/etchosts"

      ret = atmos_create_object(host, subtenant_id, @token, shared_secret, remote_file_name, local_file_name)
      puts "---atmos createobject: " + ret

      ret = atmos_read_object(host, subtenant_id, @token, shared_secret, remote_file_name, local_temp_file_name)  
      puts "---atmos readobject: " + ret

      diff = `diff #{local_file_name} #{local_temp_file_name}`
      puts "diff: " + diff
      file_same = diff == ""
      file_same.should == true
      
      # cleanup, important for later test
      `rm -f #{local_temp_file_name}`
    end
 
    after :all do
      @atmos_helper.deleteSubtenant(@subtenant_name_p)
      @atmos_helper.deleteSubtenant(@subtenant_name_p1)
    end
  end

  describe "multi-tenancy" do
    before :all do
      @raw_conf = get_raw_config()
      @subtenant_name1 = UUIDTools::UUID.random_create.to_s
      @subtenant_name2 = UUIDTools::UUID.random_create.to_s
      @token = UUIDTools::UUID.random_create.to_s
    end

    it "should isolate between different subtenants" do
      subtenant_id1 = @atmos_helper.createSubtenant(@subtenant_name1)
      subtenant_id2 = @atmos_helper.createSubtenant(@subtenant_name2)
      subtenant_id1.should_not be_nil
      subtenant_id2.should_not be_nil

      shared_secret1 = @atmos_helper.createUser(@token, @subtenant_name1)
      shared_secret2 = @atmos_helper.createUser(@token, @subtenant_name2)
      shared_secret1.should_not be_nil
      shared_secret2.should_not be_nil

      host = @raw_conf[:atmos][:host]
      remote_file_name = "etchosts"
      local_file_name = "/etc/hosts"
      local_temp_file_name = "/tmp/etchosts"

      ret = atmos_create_object(host, subtenant_id1, @token, shared_secret2, remote_file_name, local_file_name)
      puts "---atmos createobject: " + ret

      ret = atmos_read_object(host, subtenant_id1, @token, shared_secret2, remote_file_name, local_temp_file_name)
      puts "---atmos readobject: " + ret

      if File.exist?("#{local_temp_file_name}") then
        diff = `diff #{local_file_name} #{local_temp_file_name}`
        puts "diff: " + diff
        file_diff = diff != ""
        file_diff.should == true
        # cleanup, important for later test
        `rm -f #{local_temp_file_name}`
      end
    end

    after :all do
      @atmos_helper.deleteSubtenant(@subtenant_name1)
      @atmos_helper.deleteSubtenant(@subtenant_name2)
    end
  end

  describe "null credential" do
    before :all do
      @raw_conf = get_raw_config()
      @subtenant_name = UUIDTools::UUID.random_create.to_s
      @token = UUIDTools::UUID.random_create.to_s
    end

    it "should prevent null credential from login" do
      subtenant_id = @atmos_helper.createSubtenant(@subtenant_name)
      subtenant_id.should_not be_nil

      shared_secret = @atmos_helper.createUser(@token, @subtenant_name)
      shared_secret.should_not be_nil

      host = @raw_conf[:atmos][:host]
      remote_file_name = "etchosts"
      local_file_name = "/etc/hosts"
      local_temp_file_name = "/tmp/etchosts"

      ret = atmos_create_object(host, subtenant_id, @token, "", remote_file_name, local_file_name)
      puts "---atmos createobject: " + ret

      ret = atmos_read_object(host, subtenant_id, @token, "", remote_file_name, local_temp_file_name)
      puts "---atmos readobject: " + ret

      if File.exist?("#{local_temp_file_name}") then
        diff = `diff #{local_file_name} #{local_temp_file_name}`
        puts "diff: " + diff
        file_diff = diff != ""
        file_diff.should == true
        # cleanup, important for later test
        `rm -f #{local_temp_file_name}`
      end
    end

    after :all do
      @atmos_helper.deleteSubtenant(@subtenant_name)
    end
  end

  describe "unprovision" do
    before :all do
      @subtenant_name_up = UUIDTools::UUID.random_create.to_s
      @atmos_helper.createSubtenant(@subtenant_name_up)
    end

    it "should successfully delete atmos subtenant" do
      puts "subtenant_name: " + @subtenant_name_up
      success = @atmos_helper.deleteSubtenant(@subtenant_name_up)
      success.should == true
    end
  end

end
