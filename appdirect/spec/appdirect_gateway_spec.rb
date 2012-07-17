# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift(File.dirname(__FILE__))
require 'spec_helper'
require 'appdirect/appdirect_async_gateway'

module VCAP
  module Services
    module AppDirect
      class AsynchronousServiceGateway
        attr_reader :logger
      end
    end
  end
end


describe "AppDirect Gateway" do
  include Rack::Test::Methods

  def app
    @gw = VCAP::Services::AppDirect::AsynchronousServiceGateway.new(@config)
  end

  before :all do
    @config = load_config
    @rack_env = {
      "CONTENT_TYPE" => Rack::Mime.mime_type('.json'),
      "HTTP_X_VCAP_SERVICE_TOKEN" =>  @config[:token],
    }
    @api_version = "poc"
    @api = "#{@config[:appdirect][:scheme]}://#{@config[:appdirect][:host]}/api"
  end

  before do
    stub_fixture(:get, @api, VCAP::Services::AppDirect::Helper::OFFERINGS_PATH, "urbanairship/")
    stub_cc_request(:post, "services/v1/offerings", "urbanairship/")
    stub_fixture(:post, @api, VCAP::Services::AppDirect::Helper::SERVICES_PATH, "urbanairship/")
  end


  it "should add the 2 service offerings" do
    get "/", params = {}, rack_env = @rack_env
    last_response.should be_ok
    json = JSON.parse(last_response.body)
    json["offerings"].keys.should include "mongolab"
    json["offerings"].keys.should include "urbanairship"
  end

  it "should return 400 unless token" do
    get "/", params = {}, rack_env = {}
    last_response.status.should == 400
    last_response.should_not be_ok
  end

  it "should respond to create_service" do
    @svc_params = {
        :label => "mongolab-production",
        :name => "mymongo",
        :plan => "small",
        :email => "mwilkinson@vmware.com"
    }
    post "/gateway/v1/configurations", params = @svc_params.to_json, rack_env = @rack_env

    puts last_response.body.inspect
    last_response.should be_ok
    json = JSON.parse(last_response.body)
  end

end
