$:.unshift(File.dirname(__FILE__))
require_relative "spec_helper"
require_relative "../../appdirect/lib/appdirect/appdirect_helper"

describe VCAP::Services::AppDirect::Helper do

  before do
    @config = load_config
    @logger = @config[:logger]
    @appdirect = VCAP::Services::AppDirect::Helper.new(@config, @logger)

    @api = "#{@config[:appdirect][:scheme]}://#{@config[:appdirect][:host]}/api"
    @user_id = "1"
  end

  context "Urban Airship" do
      before do
        stub_fixture(:get, @api, VCAP::Services::AppDirect::Helper::OFFERINGS_PATH, "urbanairship/")
      end

      it "get_catalog should get Urban Airship in the catalog" do
        catalog = @appdirect.get_catalog
        catalog.should_not be_nil
        catalog.keys.count.should == 2
        catalog["urbanairship"]["name"].should == "Urban Airship"
      end
    end

  context "New Relic" do
    before do
      stub_fixture(:get, @api, VCAP::Services::AppDirect::Helper::OFFERINGS_PATH, "newrelic/")
    end

    it "get_catalog should get New Relic and not Mongo in the catalog" do
      catalog = @appdirect.get_catalog
      catalog.should_not be_nil

      catalog.keys.should_not include "mongolab_production"
      catalog.keys.should include "newrelic_production"
    end
  end

  context "MongoLab" do
    before do
      @scenario = "mongolab/"
      @order_id = "2"
    end

    it "get_catalog should get MongoLab in the catalog" do
      stub_fixture(:get, @api, VCAP::Services::AppDirect::Helper::OFFERINGS_PATH, @scenario)
      catalog = @appdirect.get_catalog
      catalog.should_not be_nil

      catalog.keys.should include "mongolab_production"
    end

    it "purchase_service should return the service info" do
      req = stub_fixture(:post, @api, VCAP::Services::AppDirect::Helper::SERVICES_PATH, @scenario)
      receipt = @appdirect.purchase_service(req)
      receipt.should_not be_nil
      receipt["offering"]["id"].should ==  "mongolab_production"
      receipt["uuid"].should_not be_nil
      receipt["id"].should_not be_nil
    end

    it "bind_service should return the db info" do
      req = stub_fixture(:post, @api, "#{VCAP::Services::AppDirect::Helper::SERVICES_PATH}/#{@order_id}/bindings", @scenario)
      receipt = @appdirect.bind_service(req, @order_id)
      receipt.should_not be_nil
      receipt["uuid"].should_not be_nil
      receipt["credentials"].should_not be_nil
    end

    it "unbind_service should delete the binding" do
      stub_fixture(:delete, @api, "#{VCAP::Services::AppDirect::Helper::SERVICES_PATH}/#{@order_id}/bindings/9", @scenario)
      receipt = @appdirect.unbind_service("9", @order_id)
      receipt.should be_true
    end

    it "cancel_service should return the cancellation info" do
      stub_fixture(:delete, @api, "#{VCAP::Services::AppDirect::Helper::SERVICES_PATH}/#{@order_id}", @scenario)
      cancel_receipt = @appdirect.cancel_service(@order_id)
      cancel_receipt.should be_true
    end
  end

end