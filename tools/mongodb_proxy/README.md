[ mognodb proxy ]

A proxy server to monitor mongodb disk usage and memory usage.

NOTE: For mongodb 2.0.6 version, mongodb process would crash if it fails to
      allocate disk file when it wants to flush journal. And, if memory runs
      out, mmap returns MAP_FAILED, mongodb process would crash, either.

[ how to build ]

1. env settings
    GOPATH="<working directory>/vcap-services/tools/mongodb_proxy"
    export GOPATH

2. install dependencies
    go get github.com/xushiwei/goyaml
    go get github.com/moovweb/log4go
    go get github.com/xushiwei/mgo/src/labix.org/v2/mgo

3. go build
    # env settings

    GOPATH="<working directory>/vcap-services/tools/mongodb_proxy"
    export GOPATH

    # build
    go install proxyctl

    The executable binary is located at $GOPATH/bin

4. go test
    NOTE: Please manually boot up the mongod process and set database user account first.

    cd <working directory>/vcap-services/tools/mongodb_proxy/src/go-mongo-proxy/proxy/
    go test

[ how to run ]

export CONFIG_PATH="<working directory>/vcap-services/tools/mongodb_proxy"
$GOPATH/bin/proxyctl -c $CONFIG_PATH/config/proxy.yml -p <mongo db user password>
