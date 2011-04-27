$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')
$LOAD_PATH.unshift(File.expand_path("../../../base/lib", __FILE__))

require "spec_helper"
require "atmos_service/atmos_provisioner"
require "atmos_service/atmos_helper"
require "uuidtools"

require "atmos_rest_client"

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

      opts = {
        :url => "http://" + host + ":443",
        :sid => subtenant_id,
        :uid => @token,
        :key => shared_secret,
      }
      client = AtmosClient.new(opts)
      obj = UUIDTools::UUID.random_create.to_s
      res = client.createObj(obj)
      id = res['location']
      puts "object: " + obj + " created at: #{id}"
      res = client.getObj(id)
      puts "response of reading object: #{res.body}"
      obj_same = obj == res.body
      obj_same.should == true

      res = client.deleteObj(id)
      puts "response of deleting file: #{res}"
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

      opts = {
        :url => "http://" + host + ":443",
        :sid => subtenant_id1,
        :uid => @token,
        :key => shared_secret2,
      }
      client = AtmosClient.new(opts)
      res = client.createObj("obj")
      puts res.to_s
      same_class = res == Net::HTTPForbidden || res['location'] == nil
      same_class.should == true

      opts = {
        :url => "http://" + host + ":443",
        :sid => subtenant_id2,
        :uid => @token,
        :key => shared_secret1,
      }
      client = AtmosClient.new(opts)
      res = client.createObj("obj")
      puts res.to_s
      same_class = res == Net::HTTPForbidden || res['location'] == nil
      same_class.should == true
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
    end

    it "should prevent null credential from login" do
      subtenant_id = @atmos_helper.createSubtenant(@subtenant_name)
      subtenant_id.should_not be_nil
      host = @raw_conf[:atmos][:host]

      opts = {
        :url => "http://" + host + ":443",
        :sid => subtenant_id,
        :uid => "",
        :key => "",
      }
      client = AtmosClient.new(opts)
      res = client.createObj("obj")
      puts res.to_s
      same_class = res == Net::HTTPForbidden || res['location'] == nil
      same_class.should == true
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
