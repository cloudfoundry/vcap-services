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
  end

  it "should add the 2 service offerings in the cloud controller" do
    EM.run do
      get "/", params = {}, rack_env = @rack_env
      EM.stop
    end
  end

end
