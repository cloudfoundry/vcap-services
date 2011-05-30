# Copyright (c) 2009-2011 VMware, Inc.
require 'httpclient'

Given /^I deploy a service demo application using the "([^""]*)" service$/ do |service|
  expected_health = 1.0
  @app = SERVICE_TEST_APP
  @app_detail = create_app(@app, @token)
  case service
  when "mysql"
    @service_detail = provision_mysql_service(@token)
    debug "service_detail #{@service_detail}"
  else
    fail "Unknown service #{service}"
  end
  upload_app @app, @token
  bind_service_to_app @app_detail, @service_detail, @token
  start_app @app, @token
  health = poll_until_done @app, expected_health, @token
  health.should == expected_health
end

When /^I backup "([^""]*)" service$/ do |service|
  case service
  when "mysql"
    mysql_backup @service_detail
  else
    fail "Unknown service #{service}. Valide services:#{valid_services}"
  end
end

When /^I add (\d+) user records to service demo application/ do |records|
  1.upto records.to_i do |i|
    uri = get_uri @app, "user/#{i}"
    uri = "http://" + uri
    debug "URI: #{uri}"
    response = HTTPClient.post uri, ""
    response.status.should == 302
  end
end



When /^I shutdown "([^""]*)" node$/ do |service|
  if valid_services.include? service
    shutdown_service_node service
  else
    fail "Unknown service #{service}. Valide services:#{valid_services}"
  end
end

When /^I delete the service from the local database of "([^""]*)" node$/ do |service|
  case service
  when "mysql"
    mysql_drop_service_from_db
  else
    fail "Unknown service #{service}. Valide services:#{valid_services}"
  end
end

When /^I restart the application$/ do
  expected_health = 1.0
  stop_app @app, @token
  start_app @app, @token
  health = poll_until_done @app, expected_health, @token
end

When /^I delete the service from "([^""]*)" node$/ do |service|
  case service
  when "mysql"
    mysql_drop_service
  else
    fail "Unknown service #{service}. Valide services:#{valid_services}"
  end
end

When /^I start "([^""]*)" node$/ do |service|
  if valid_services.include? service
    start_service_node service
    # Wait until node is ready.
    sleep 10
  else
    fail "Unknown service #{service}. Valide services:#{valid_services}"
  end
end

When /^I recover "([^""]*)" service$/ do |service|
  case service
  when "mysql"
    mysql_recover
  else
    fail "Unknown service #{service}. Valide services:#{valid_services}"
  end
end

Then /^I should have the same (\d+) user records on demo application$/ do |records|
  uri = get_uri @app, "user"
  response = HTTPClient.get "http://"+uri
  response.status.should == 200
  content = response.content
  users = content.split("\n")
  debug "Output #{users.inspect}"
  1.upto records.to_i do |i|
    users.should include i.to_s
  end
end
