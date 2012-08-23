## Overview

This AppDirect Gateway acts as a proxy between the Cloud Controller and App Direct to list and provision services.

## Functionality

### On startup

- Reads the list of services available in App Direct and the list of services available in Cloud Controller
  - See https://wiki.springsource.com/display/ACDEV/VCAP+Service+Gateway+API+Document for how to read from CC the list of services
- Adds, updates or removes(deactivates) AppDirect services listed in Cloud Controller
- This behavior is repeated on a periodic basis

### On create-service, delete-service, bind-service, unbind-service

- Maps and forwards the requests to and from AppDirect

### Requirements to run tests

- Access to the internet for rubygems, github, appdirect
- Ruby 1.9.3p125

### Testing

``` bash

$ bundle install
$ rspec spec

```


### Runnning standalone

```bash
$ ./bin/appdirect_gateway

 ```

### Running in dev_setup


### Running on bosh

