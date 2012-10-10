package main

import (
	"flag"
	"fmt"
	"go-mongo-proxy/proxy"
	"os"
)

import l4g "github.com/moovweb/log4go"

var log l4g.Logger
var config_path string

func main() {
	flag.StringVar(&config_path, "c", "", "proxy config file")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s -c <config_file>\n", os.Args[0])
		os.Exit(-1)
	}

	flag.Parse()
	if flag.NArg() < 1 && (config_path == "") {
		flag.Usage()
	}

	conf := load_config(config_path)

	log = make(l4g.Logger)
	log_init(log, conf)
	defer log_fini(log)

	proxy.StartProxyServer(conf, log)
}
