#!/usr/bin/env bash

set -x -e

CWD=`pwd`

export GOPATH=$CWD

go get github.com/xushiwei/goyaml
go get github.com/moovweb/log4go
go get github.com/xushiwei/mgo/src/labix.org/v2/mgo

go install proxyctl
