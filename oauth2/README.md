The project is a cloudfoundry service gateway exposing the kernel
[UAA][].  When you provision it (`vmc create-service`) an OAuth2
client registration is created in the UAA.  Then when you bind an
application to it (`vmc bind-service`), the app gets some credentials
in `VCAP_SERVICES` environment variable, e.g.

    VCAP_SERVICES={"oauth2-1.0":[{"name":"oauth2", "label":"oauth2-1.0",
      "plan":"free", "tags":["uaa", "oauth2-1.0", "oauth2"], "credentials":
      {"auth_server_url":"http://login.cloudfoundry.com", "token_server_url":"http://uaa.cloudfoundry.com", 
      "client_id":"b1366591-5456-4221-8563-9f8370ead694",
      "client_secret":"af6c147d-5695-495a-bfdc-e7132c8b1dd2"}}]}

The application can use the "credentials" field to drive an
authorization code flow and obtain an OAuth2 access token.  The
default scope for a token is
`["openid", "cloud_controller.read", "cloud_controller.write"]` which
gives the application the ability to authenticate a user and obtain
basic profile information, and also to manage the users applications
and services in the cloud controller.

[UAA]: http://github.com/cloudfoundry/uaa

## Typical Log Output

### Provision

    [2012-11-04 19:02:03.134148] gateway - pid=8970 tid=6f89 fid=17af  DEBUG -- Provision request for label=test-1.0, plan=free, version=1.0
    [2012-11-04 19:02:03.134406] gateway - pid=8970 tid=6f89 fid=17af  DEBUG -- [Test-Provisioner] Attempting to provision instance (request={:label=>"test-1.0", :name=>"test", :email=>"vcap_tester@vmware.com", :plan=>"free", :version=>"1.0"})
    [2012-11-04 19:02:03.134858] gateway - pid=8970 tid=6f89 fid=17af  DEBUG -- Provisioned {:configuration=>{:plan=>"free", :version=>"1.0"}, :service_id=>"b5df21d7-ccfd-4a8e-adbe-55a2c3172de9", :credentials=>{"internal"=>{"name"=>"b5df21d7-ccfd-4a8e-adbe-55a2c3172de9"}}}
    [2012-11-04 19:02:03.135036] gateway - pid=8970 tid=6f89 fid=17af  DEBUG -- Reply status:200, headers:{"Content-Type"=>"application/json"}, body:{"configuration":{"plan":"free","version":"1.0"},"service_id":"b5df21d7-ccfd-4a8e-adbe-55a2c3172de9","credentials":{"internal":{"name":"b5df21d7-ccfd-4a8e-adbe-55a2c3172de9"}}}

### Bind

    [2012-11-05 10:07:34.775173] gateway - pid=12579 tid=dc89 fid=23a0  DEBUG -- [Test-Provisioner] Attempting to bind to service b5df21d7-ccfd-4a8e-adbe-55a2c3172de9
    [2012-11-05 10:07:34.775694] gateway - pid=12579 tid=dc89 fid=23a0  DEBUG -- [Test-Provisioner] Bound: {:service_id=>"d5e8754d-b1dc-4562-9fce-eb7747460b89", :configuration=>{"plan"=>"free", "version"=>"1.0", "data"=>{"binding_options"=>{}}}, :credentials=>{"internal"=>{"name"=>"b5df21d7-ccfd-4a8e-adbe-55a2c3172de9"}}}
    [2012-11-05 10:07:34.775903] gateway - pid=12579 tid=dc89 fid=23a0  DEBUG -- Reply status:200, headers:{"Content-Type"=>"application/json"}, body:{"service_id":"d5e8754d-b1dc-4562-9fce-eb7747460b89","configuration":{"plan":"free","version":"1.0","data":{"binding_options":{}}},"credentials":{"internal":{"name":"b5df21d7-ccfd-4a8e-adbe-55a2c3172de9"}}}

### Unbind

    [2012-11-05 10:06:52.458826] gateway - pid=12579 tid=dc89 fid=23a0   INFO -- Unbind request for service_id=b5df21d7-ccfd-4a8e-adbe-55a2c3172de9 handle_id=a55a4fe5-5c3e-4403-960e-c78025e35324
    [2012-11-05 10:06:52.459112] gateway - pid=12579 tid=dc89 fid=23a0  DEBUG -- [Test-Provisioner] Attempting to unbind to service b5df21d7-ccfd-4a8e-adbe-55a2c3172de9
    [2012-11-05 10:06:52.459234] gateway - pid=12579 tid=dc89 fid=23a0  DEBUG -- Reply status:200, headers:{"Content-Type"=>"application/json"}, body:{}


## Steps to Register with the Cloud Controller

### Provide a config file

To register with the cloud controller you need to provide a config
file (the default has some values in it, but won't have the right
values for your environment). Then you can try and launch with, for
instance

    $ bin/gateway -c config/dev.yml

Note that the services base code will require `/var/vcap/sys/run/LOCK`
to be writable.  This is fixed in the `dsyer` fork so that the lock
file location can be overridden with an environment variable:

    $ LOCK_FILE=/tmp/LOCK bin/gateway -c config/dev.yml

### NATS Registration

NATS registration happens before contacting the Cloud Controller so if
you have problems connecting you are hosed, but you can disable it by
*not* providing an `mbus` entry in the local config.

### Make the Cloud Controller aware of our offering

The cloud controller has to be expecting us as well, so you need this
in `cloud_controller.yml` the first time you run the gateway (but not
subsequently):

    builtin_services:
     test: 0xdeadbeef

In a BOSH deployment you can do this by adding a snippet to the
manifest and then doing a `bosh deploy`:

    external_service_tokens:
      test: 0xdeadbeef

### Make sure the gateway is not registered as "core"

The cloud controller will register the service, but you need it to be
registered wit the "core" provider, so *don't* specify that property
in the gateway config file.

### Port Numbers

If you are testing a gateway without NATS registration, the Cloud
Controller database doesn't seem to get updated with the new port when
you change the gateweay, and the default is to pick an ephemeral port.
So it's best to fix the port in the gateway YML config for testing.

## UAA Configuration

The service must be registered as a client in the UAA.  It needs to be
authorized for client credentials grants so it can create and delete
new clients, and it needs to be autoapprove for implicit grants, so it
can get a list of user's apps from the cloud controller.

The standard `uaa.yml` would have:

    oauth:
      clients:
        oauth2service:
          secret: oauth2servicesecret
          scope: openid,cloud_controller.read,cloud_controller.write
          authorities: uaa.resource,oauth.service,clients.read,clients.write,clients.secret
          authorized-grant-types: client_credentials,implicit
          redirect-uri: http://uaa.cloudfoundry.com/redirect/oauth2service # can be anything
          override: true
        # ... others
      client:
        autoapprove:
          - vmc
          - dashboard
          - oauth2service
          # ... others

A BOSH manifest would have something like this:

    clients:
      oauth2service:
        secret: oauth2servicesecret
        scope: openid,cloud_controller.read,cloud_controller.write
        authorities: uaa.resource,oauth.service,clients.read,clients.write,clients.secret
        authorized-grant-types: client_credentials,implicit
        redirect-uri: http://uaa.cloudfoundry.com/redirect/oauth2service # can be anything
        override: true
    client:
      autoapprove:
        - vmc
        - dashboard
        - oauth2service
        # ... others
