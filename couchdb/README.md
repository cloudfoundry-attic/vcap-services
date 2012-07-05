CouchDB service for CloudFoundry
================================

The couchdb service manages a single shared instance of CouchDB that is
managed (started, stopped etc) by the OS; in that it is more similar to
the MySQL or PostgreSQL services than MongoDB.

Most distributions of CouchDB come with no security by default. To run
the CF service you *must* enable mandatory security by editing your
local.ini file. Depending on the OS, you can find it in _/etc/local.ini_
(on Linux), _/usr/local/etc/local.ini_ (on FreeBSD) or _~/etc/local.ini_
(on Mac OS X with homebrew). As a minimum, you need to add:

    [couch_httpd_auth]
    require_valid_user = true

    [admins]
    admin = mysecretpassword

You also need to configure the same password for the _admin_ user in the
_couchdb\_node.yml_ config file.
