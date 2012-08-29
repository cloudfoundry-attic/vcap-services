$:.unshift(File.dirname(__FILE__))
require_relative "spec_helper"
require_relative "../../lib/marketplaces/appdirect/appdirect_helper"
require_relative "../../lib/marketplaces/appdirect/appdirect_error"

require "fiber"

describe VCAP::Services::Marketplace::Appdirect::AppdirectHelper do

  before do
    @config = load_config
    @logger = @config[:logger]
    @appdirect = VCAP::Services::Marketplace::Appdirect::AppdirectHelper.new(@config, @logger)

    @api = "#{@config[:appdirect][:scheme]}://#{@config[:appdirect][:host]}/api"
    @user_id = "1"
  end

  context "Activity Streams" do
      before do
        stub_fixture(:get, @api, VCAP::Services::Marketplace::Appdirect::AppdirectHelper::OFFERINGS_PATH, "asms_dev/")
      end

      it "get_catalog should get Activity Streams in the catalog" do
        EM.run do
          f = Fiber.new do
            @catalog = @appdirect.get_catalog
            @catalog.should_not be_nil
            @catalog.keys.count.should == 4
            @catalog["asms_dev"]["name"].should == "Activity Streams"
            EM.stop
          end
          f.resume
        end
      end

      it "purchase_service should not allow creation due to missing code from AppDirect" do
        EM.run do
          f = Fiber.new do
            req = stub_fixture(:post, @api, VCAP::Services::Marketplace::Appdirect::AppdirectHelper::SERVICES_PATH, "asms_dev/")
            lambda { receipt = @appdirect.purchase_service(req)}.should raise_error
            #(VCAP::Services::AppDirect::AppDirectError::APPDIRECT_ERROR_PURCHASE)
            EM.stop
          end
          f.resume
        end
      end
    end

  context "MongoLab" do
    before do
      @scenario = "mongolab/"
    end

    it "get_catalog should get Mongo in the catalog" do
      EM.run do
        f = Fiber.new do
          @catalog = @appdirect.get_catalog
          @catalog.should_not be_nil
          @catalog.keys.count.should == 4
          @catalog["mongolab"]["name"].should == "MongoLab"
          EM.stop
        end
        f.resume
      end
    end

    it "able to purchase and cancel service" do
      EM.run do
        f = Fiber.new do
          req = stub_fixture(:post, @api, VCAP::Services::Marketplace::Appdirect::AppdirectHelper::SERVICES_PATH, @scenario)
          puts "Posting#{req.inspect}"
          receipt = @appdirect.purchase_service(req)
          receipt.should_not be_nil
          receipt["offering"]["id"].should ==  "mongolab_dev"
          receipt["uuid"].should_not be_nil
          @order_id = receipt["uuid"]
          receipt["id"].should_not be_nil

          puts "Now cancelling the service"

          @cancel_receipt = @appdirect.cancel_service(@order_id)
          @cancel_receipt.should be_true
          EM.stop
        end
        f.resume
      end
    end

    it "able to purchase, bind and cancel service" do
      EM.run do
        f = Fiber.new do
          req = stub_fixture(:post, @api, VCAP::Services::Marketplace::Appdirect::AppdirectHelper::SERVICES_PATH, @scenario)
          puts "Posting#{req.inspect}"
          receipt = @appdirect.purchase_service(req)
          receipt.should_not be_nil
          receipt["offering"]["id"].should ==  "mongolab_dev"
          receipt["uuid"].should_not be_nil
          @order_id = receipt["uuid"]
          receipt["id"].should_not be_nil

          puts "Now binding the service"

          req = {}
          receipt = @appdirect.bind_service(req, @order_id)
          receipt.should_not be_nil
          receipt["uuid"].should_not be_nil
          receipt["credentials"].should_not be_nil

          puts "Got receipt #{receipt.inspect}"

          puts "Now cancelling the service"
          @cancel_receipt = @appdirect.cancel_service(@order_id)
          @cancel_receipt.should be_true
          EM.stop
        end
        f.resume
      end
    end

    it "able to purchase, bind, unbind and cancel service" do
      EM.run do
        f = Fiber.new do
          req = stub_fixture(:post, @api, VCAP::Services::Marketplace::Appdirect::AppdirectHelper::SERVICES_PATH, @scenario)
          puts "Posting#{req.inspect}"
          receipt = @appdirect.purchase_service(req)
          receipt.should_not be_nil
          receipt["offering"]["id"].should ==  "mongolab_dev"
          receipt["uuid"].should_not be_nil
          @order_id = receipt["uuid"]
          receipt["id"].should_not be_nil

          puts "Now binding the service"

          req = {}
          receipt = @appdirect.bind_service(req, @order_id)
          puts "Got resp #{receipt.inspect}"
          receipt.should_not be_nil
          receipt["uuid"].should_not be_nil
          @binding_id = receipt["uuid"]
          receipt["credentials"].should_not be_nil

          puts "Now unbinding service"

          unbind_receipt = @appdirect.unbind_service(@order_id, @binding_id)
          unbind_receipt.should be_true

          puts "Now cancelling the service"
          @cancel_receipt = @appdirect.cancel_service(@order_id)
          @cancel_receipt.should be_true
          EM.stop
        end
        f.resume
      end
    end

  end
end
