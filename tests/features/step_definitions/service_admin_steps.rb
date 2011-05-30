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
  @user_records ||= {}
  1.upto records.to_i do |i|
    uri = get_uri @app, "user"
    uri = "http://" + uri
    debug "URI: #{uri}"
    username = "username#{i}"
    response = HTTPClient.post uri, username
    debug "response of create user: #{response.content}"
    response.status.should == 302
    location = response.headers['Location']
    location =~ /user\/(\d+)/
    id = $1
    @user_records[id] = username
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
  health.should == expected_health
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

Then /^I should not able to read (\d+) user records on demo application$/ do |records|
  @user_records.size.should == records.to_i
  @user_records.keys.each do |id|
    uri = get_uri @app, "user/#{id}"
    response = HTTPClient.get "http://"+uri
    debug "Response of get user: #{response.content}"
    response.status.should == 500
  end
end

Then /^I should have the same (\d+) user records on demo application$/ do |records|
  @user_records.size.should == records.to_i
  @user_records.keys.each do |id|
    uri = get_uri @app, "user/#{id}"
    response = HTTPClient.get "http://"+uri
    debug "Response of get user: #{response.content}"
    response.status.should == 200
    response.content.should == @user_records[id]
  end
end
