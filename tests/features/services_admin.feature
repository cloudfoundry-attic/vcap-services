# Copyright (c) 2009-2011 VMware, Inc.

Feature: Service admin tasks
  As service admin or operator
  I want to perform admin tasks on serivces

  @creates_service_test_app @provisions_service
  Scenario: Recover a mysql instance
    Given I have registered and logged in
    Given I deploy a service demo application using the "mysql" service
    When I add 3 user records to service demo application
    Then I should have the same 3 user records on demo application
    When I backup "mysql" service
    When I shutdown "mysql" node
    When I delete the service from "mysql" node
    When I delete the service from the local database of "mysql" node
    When I start "mysql" node
    Then I should not able to read 3 user records on demo application
    When I recover "mysql" service
    # Restart application in case application have cache
    When I restart the application
    Then I should have the same 3 user records on demo application
