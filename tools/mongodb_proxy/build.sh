#!/usr/bin/env bash

set -x -e

CWD=`pwd`

export GOPATH=$CWD:$CWD/../../govendor

go install proxyctl
