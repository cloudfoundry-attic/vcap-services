package main

import (
	"github.com/xushiwei/goyaml"
	"go-mongo-proxy/proxy"
	"io/ioutil"
)

var conf proxy.ProxyConfig

func load_config(path string) (config *proxy.ProxyConfig) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}

	err = goyaml.Unmarshal([]byte(data), &conf)
	if err != nil {
		panic(err)
	}
	return &conf
}
