$:.unshift(File.dirname(__FILE__))
require_relative "spec_helper"
require_relative "../../appdirect/lib/appdirect/appdirect_helper"

describe VCAP::Services::AppDirect::Helper do

  before do
    @config = load_config
    @logger = @config[:logger]
    @appdirect = VCAP::Services::AppDirect::Helper.new(@config, @logger)

    @api = "https://cloudfoundry.appdirect.com/api"
    @user_id = "1"
  end

  context "New Relic" do
    before do
      stub_fixture(:get, @api, "marketplace/v1/listing", "newrelic/")
    end

    it "get_catalog should get New Relic and not Mongo in the catalog" do
      catalog = @appdirect.get_catalog
      catalog.should_not be_nil

      catalog.keys.should_not include "Mongo Lab"
      catalog.keys.should include "New Relic"
    end
  end

  context "Mongo Lab" do
    before do
      @scenario = "mongolab/"
      @order_id = "2"
    end

    it "get_catalog should get MongoLab in the catalog" do
      stub_fixture(:get, @api, "marketplace/v1/listing", @scenario)
      catalog = @appdirect.get_catalog
      catalog.should_not be_nil

      catalog.keys.should include "Mongo Lab"
    end

    it "purchase_service should return the service info" do
      req = stub_fixture(:post, @api, "account/v1/users/1/orders", @scenario)
      receipt = @appdirect.purchase_service(req, @user_id)
      receipt.should_not be_nil
      receipt["productId"].should ==  "mongolab"
      receipt["order"].should_not be_nil
    end

    it "bind_service should return the db info" do
      req = stub_fixture(:post, @api, "account/v1/users/1/orders/2/apps", @scenario)
      receipt = @appdirect.bind_service(req, @user_id, @order_id)
      receipt.should_not be_nil
      receipt["productId"].should ==  "mongolab"
      receipt["order"].should_not be_nil
    end

    it "unbind_service should delete the binding" do
      stub_fixture(:delete, @api, "account/v1/users/1/orders/2/apps/9", @scenario)
      receipt = @appdirect.unbind_service("9", @user_id, @order_id)
      receipt.should_not be_nil
      receipt["productId"].should ==  "mongolab"
      receipt["order"].should_not be_nil
    end

    it "cancel_service should return the cancellation info" do
      stub_fixture(:delete, @api, "account/v1/users/1/orders/2", @scenario)
      cancel_receipt = @appdirect.cancel_service(@user_id, @order_id)
      cancel_receipt.should_not be_nil
      cancel_receipt["productId"].should_not be_nil
      cancel_receipt["cancelled"].should be_true
    end
  end

end