#
# The test automation based on Cucumber uses the steps defined and implemented here to
# facilitate the handling of the various scenarios that make up the feature set of
# AppCloud.
#
# Author:: A.B.Srinivasan
# Copyright:: Copyright (c) 2010 VMware Inc.

#World(AppCloudHelper)

World do
  AppCloudHelper.instance
end

## User management

# Register
Given /^I am a new user to AppCloud$/ do
  pending "new user registration is temporarily disabled in the bvts"
  AppCloudHelper.instance.create_user
end

When /^I register$/ do
  @user = AppCloudHelper.instance.register
end

Then /^I should be able to login to AppCloud\.$/ do
  @user.should_not == nil
  AppCloudHelper.instance.login
end

# Login
Given /^I am registered$/ do
  user = AppCloudHelper.instance.get_registered_user
  if (user == nil)
    user = AppCloudHelper.instance.create_user
    AppCloudHelper.instance.register
  end
  user.should_not == nil
end

When /^I login$/ do
  @token = AppCloudHelper.instance.login
end

Then /^I should get an authentication token that I need to use with all subsequent AppCloud requests$/ do
  @token.should_not == nil
end

# Re-login
Given /^I have logged in$/ do

  user = AppCloudHelper.instance.get_registered_user
  if (user == nil)
    user = AppCloudHelper.instance.create_user
    AppCloudHelper.instance.register
  end
  user.should_not == nil

  @first_login_token = AppCloudHelper.instance.get_login_token
  @first_login_token.should_not == nil
end

Then /^I should get a new authentication token that I need to use for all subsequent AppCloud requests$/ do
  @token.should_not == nil
  @token.should_not == @first_login_token
end

## Application CRUD operations

Given /^I have registered and logged in$/ do
  user = AppCloudHelper.instance.get_registered_user
  if user == nil
    user = AppCloudHelper.instance.create_user
    AppCloudHelper.instance.register
  end
  user.should_not == nil

  @token = AppCloudHelper.instance.get_login_token
  if @token == nil
    @token = AppCloudHelper.instance.login
  end
  @token.should_not == nil
end

# Create
When /^I create a simple application$/ do
  @app = create_app SIMPLE_APP, @token
end

Then /^I should have my application on AppCloud$/ do
  @app.should_not == nil
end

Then /^it should not be started$/ do
  status = get_app_status @app, @token
  status.should_not == nil
  status['state'].should_not == 'STARTED'
end

# Read (Query status)
Given /^I have my simple application on AppCloud$/ do
  @app = create_app SIMPLE_APP, @token
end

When /^I query status of my application$/ do
  @status = get_app_status @app, @token
end

Then /^I should get the state of my application$/ do
  @status.should_not == nil
end

# Delete
When /^I delete my application$/ do
  delete_app @app, @token
end

Then /^it should not be on AppCloud$/ do
  status = get_app_status @app, @token
  status.should == nil
end

# Upload
When /^I upload my application$/ do
  upload_app @app, @token
end

## Application availability control
# Start
When /^I start my application$/ do
  start_app @app, @token
end

Then /^it should be started$/ do
  status = get_app_status @app, @token
  status.should_not == nil
  status['state'].should == 'STARTED'
  expected_health = 1.0
  health = poll_until_done @app, expected_health, @token
  health.should == expected_health
end

Then /^it should be available for use$/ do
  contents = get_app_contents @app
  contents.should_not == nil
  contents.body_str.should_not == nil
  contents.body_str.should =~ /Hello from VCAP/
  contents.close
end

# Stop
When /^I stop my application$/ do
  stop_app @app, @token
end

Then /^it should be stopped$/ do
  status = get_app_status @app, @token
  status.should_not == nil
  status['state'].should == 'STOPPED'
  expected_health = 0.0
  health = poll_until_done @app, expected_health, @token
  health.should == expected_health
end

Then /^it should not be available for use$/ do
  contents = get_app_contents @app
  contents.should_not == nil
  contents.response_code.should == 404
  contents.close
end

# List apps
Given /^I have deployed a simple application$/ do
  @app = create_app SIMPLE_APP, @token
  upload_app @app, @token
  start_app @app, @token
  expected_health = 1.0
  health = poll_until_done @app, expected_health, @token
  health.should == expected_health
end

Given /^I have built a simple Erlang application$/ do
  # Try to find an appropriate Erlang
  erlang_path = '/var/vcap/runtimes/erlang-R14B02/bin'
  unless File.exists?(erlang_path)
    pending "Not running Erlang test because the Erlang runtime is not installed"
  else
    Dir.chdir("#{@testapps_dir}/#{SIMPLE_ERLANG_APP}")
    make_prefix = "PATH=#{erlang_path}:$PATH"
    rel_build_result = `#{make_prefix} make relclean rel`
    raise "Erlang application build failed: #{rel_build_result}" if $? != 0
  end
end

Given /^I have deployed a simple Erlang application$/ do
  @app = create_app SIMPLE_ERLANG_APP, @token
  upload_app @app, @token, "rel/mochiweb_test"
  start_app @app, @token
  expected_health = 1.0
  health = poll_until_done @app, expected_health, @token
  health.should == expected_health
end

Given /^I have deployed a tiny Java application$/ do
  @java_app = create_app TINY_JAVA_APP, @token
  upload_app @java_app, @token
  start_app @java_app, @token
  expected_health = 1.0
  health = poll_until_done @java_app, expected_health, @token
  health.should == expected_health
end

When /^I list my applications$/ do
  @app_list = list_apps @token
  @app_list.should_not == nil
end

Then /^I should get status on the simple app as well as the tiny Java application$/ do
  simple_app = get_app_info @app_list, @app
  tiny_java_app = get_app_info @app_list, @java_app
  simple_app.should_not == nil
  tiny_java_app.should_not == nil
end

# Get app files
When /^I list files associated with my application$/ do
  @instance = '0'
  path = '/'
  @response = get_app_files @app, @instance, path, @token
end

Then /^I should get a list of directories and files associated with my application on AppCloud$/ do
  @response.status.should == 200
  @response.content.should_not == nil
end

Then /^I should be able to retrieve any of the listed files$/ do
  @instance = '0'
  path = '/app'
  response = get_app_files @app, @instance, path, @token
  response.status.should == 200
end

# Get instances info
Given /^I have (\d+) instances of a simple application$/ do |arg1|
  @instances = set_app_instances @app, arg1.to_i, @token
end

When /^I get instance information for my application$/ do
  @instances_info = get_instances_info @app, @token
end

Then /^I should get status on all instances of my application$/ do
  @instances_info.should_not == nil
  @instances_info['instances'].length.should == @instances
end

# Get crash info
Given /^I that my application has a crash$/ do
  @instance = '0'
  path = '/run.pid'
  response = get_app_files @app, @instance, path, @token
  response.status.should == 200
  pid = response.content.chomp
  # This call causes the app to crash
  begin
    contents = get_app_contents @app, "crash/#{pid}"
    contents.close
  rescue
  end
end

When /^I get crash information for my application$/ do
  @crash_info = get_app_crashes @app, @token
end

Then /^I should be able to get the time of the crash from that information$/ do
  @crash_info.should_not == nil
  Time.at(@crash_info['crashes'][0]['since']).should_not == nil
end

Then /^I should be able to get a list of files associated with my application on AppCloud$/ do
  @instance = '0'
  path = '/'
  @response = get_app_files @app, @instance, path, @token
  @response.status.should == 200
  @response.content.should_not == nil
end

# Crash info for a broken (persistently broken) app
Given /^I have deployed a broken application$/ do
  @app = create_app BROKEN_APP, @token
  upload_app @app, @token
  start_app @app, @token
  sleep 3
end

# Resource use
When /^I get resource usage for my application$/ do
  @app_stats = get_app_stats @app, @token
end

Then /^I should get information representing my application\'s resource use\.$/ do
  @app_stats.should_not == nil
  stats = @app_stats.to_a[0][1]['stats']
  stats.should_not == nil
  appname = get_app_name @app
  stats['name'].should == appname

  timeout = 6 # Because monitor sweeps are 5 secs..
  sleep_time = 0.5

  while stats['usage'] == nil && timeout > 0
    sleep sleep_time
    timeout -= sleep_time
    @app_stats = get_app_stats @app, @token
    stats = @app_stats.to_a[0][1]['stats']
  end

  stats['usage'].should_not == nil
end

# Update app instance count
When /^I increase the instance count of my application by (\d+)$/ do |arg1|
  instances_info = get_instances_info @app, @token
  instances_info.should_not == nil
  set_app_instances @app, instances_info['instances'].length + arg1.to_i, @token
end

Then /^I should have (\d+) instances of my application$/ do |arg1|
  instances_info = get_instances_info @app, @token
  instances_info.should_not == nil
  instances_info['instances'].length.should == arg1.to_i
end

When /^I decrease the instance count of my application by (\d+)$/ do |arg1|
  instances_info = get_instances_info @app, @token
  instances_info.should_not == nil
  set_app_instances @app, instances_info['instances'].length - arg1.to_i, @token
end

# Map & unmap application URIs
When /^I add a url to my application$/ do
  app_info = get_app_status @app, @token
  app_info.should_not == nil
  uris = app_info['uris']
  @original_uri = uris[0]
  appname = get_app_name @app
  @new_uri = create_uri "#{appname}-1"
  add_app_uri @app, @new_uri, @token
end

# Map & unmap application URIs
When /^I add a url that differs only by case$/ do
  # While odd, this is allowed for a single user.  It should fail
  # for similar urls, both in terms of the same case and mixed
  # case across users.  These tests aren't setup for
  # cross user testing at the moment.  For a single user we might
  # merge these urls on the backend, but we don't for the moment,
  # hence the 'pending' status below.
  pending "the expected behavior of this test is under discussion"
  app_info = get_app_status @app, @token
  app_info.should_not == nil
  uris = app_info['uris']
  uris.length.should == 1
  @original_uri = uris[0]
  appname = get_app_name @app
  @new_uri = create_uri "#{appname.swapcase}"
  @new_uri.should_not == @original_uri
  add_app_uri @app, @new_uri, @token
end

Then /^I should have (\d+) urls associated with my application$/ do |arg1|
  app_info = get_app_status @app, @token
  app_info.should_not == nil
  uris = app_info['uris']
  uris.length.should == arg1.to_i
end

Then /^I should be able to access the application through the original url\.$/ do
  contents = get_uri_contents @original_uri
  contents.should_not == nil
  contents.body_str.should_not == nil
  contents.body_str.should =~ /Hello from VCAP/
  contents.close
end

Then /^I should be able to access the application through the new url\.$/ do
  # Time dependent, so sleep for a small amount.
  sleep 0.25

  contents = get_uri_contents @new_uri
  contents.should_not == nil
  contents.body_str.should_not == nil
  contents.body_str.should =~ /Hello from VCAP/
  contents.close
end

Given /^I have my application associated with '(\d+)' urls$/ do |arg1|
  app_info = get_app_status @app, @token
  app_info.should_not == nil
  uris = app_info['uris']
  @remaining_uri = uris[0]
  appname = get_app_name @app
  @uri_to_be_removed = appname << "-1"
  @uri_to_be_removed = create_uri @uri_to_be_removed
  add_app_uri @app, @uri_to_be_removed, @token
end

When /^I remove one of the urls associated with my application$/ do
  remove_app_uri @app, @uri_to_be_removed, @token
end

Then /^I should be able to access the application through the remaining url\.$/ do
  contents = get_uri_contents @remaining_uri
  contents.should_not == nil
  contents.body_str.should_not == nil
  contents.body_str.should =~ /Hello from VCAP/
  contents.close
end

Then /^I should be not be able to access the application through the removed url\.$/ do
  # Time dependent, so sleep for a small amount.
  sleep 0.25

  contents = get_uri_contents @uri_to_be_removed
  contents.should_not == nil
  contents.response_code.should == 404
  contents.close
end

When /^I remove the original url associated with my application$/ do
  remove_app_uri @app, @original_uri, @token
end

Then /^I should be not be able to access the application through the original url\.$/ do
  contents = get_uri_contents @original_uri
  contents.should_not == nil
  contents.response_code.should == 404
  contents.close
end

# Modify application contents
When /^I upload a modified simple application to AppCloud$/ do
  modify_and_upload_app @app, @token
end

When /^I update my application on AppCloud$/ do
  @response = poll_until_update_app_done @app, @token
end

Then /^my update should succeed$/ do
  @response.should == 'SUCCEEDED'
end

Then /^I should be able to access the updated version of my application$/ do
  contents = get_app_contents @app
  contents.should_not == nil
  contents.body_str.should_not == nil
  contents.body_str.should =~ /Hello from modified VCAP/
  contents.close
end

Then /^I should be able to access the original version of my application$/ do
  pending
  contents = get_app_contents @app
  contents.should_not == nil
  contents.body_str.should_not == nil
  contents.body_str.should =~ /Hello from VCAP/
  contents.close
end

# Simple Sinatra CRUD application that uses MySQL
Given /^I deploy my simple application that is backed by the MySql database service on AppCloud$/ do
  @app = create_app SIMPLE_DB_APP, @token
  @service = provision_db_service @token
  attach_provisioned_service @app, @service, @token
  upload_app @app, @token
  start_app @app, @token
  expected_health = 1.0
  health = poll_until_done @app, expected_health, @token
  health.should == expected_health
end

When /^I add a record to my application$/ do
  @desc = "Description"
  @id = "tester1"
  data_hash = { :id => @id, :desc => @desc}
  uri = get_uri @app, "users"
  post_record uri, data_hash
end

Then /^I should be able to retrieve the record that was added$/ do
  user_id = @id
  uri = get_uri @app, "users/#{user_id}"
  contents = get_uri_contents uri
  contents.should_not == nil
  user_hash = parse_json contents.body_str
  user_hash['id'].should == user_id
  user_hash['desc'].should == @desc
  contents.close
end

Then /^be able to update the record$/ do
  updated_desc = "Updated description"
  data_hash = { :id => @id, :desc => updated_desc}
  uri = get_uri @app, "users/#{@id}"
  put_record uri, data_hash

  uri = get_uri @app, "users/#{@id}"
  contents = get_uri_contents uri
  contents.should_not == nil
  user_hash = parse_json contents.body_str
  user_hash['id'].should == @id
  user_hash['desc'].should == updated_desc
  contents.close
end

Then /^be able to delete the record$/ do
  uri = get_uri @app, "users/#{@id}"
  contents = get_uri_contents uri
  contents.should_not == nil
  contents.close
  delete_record uri
  contents = get_uri_contents uri
  contents.should_not == nil
  contents.response_code.should == 404
  contents.close
end

