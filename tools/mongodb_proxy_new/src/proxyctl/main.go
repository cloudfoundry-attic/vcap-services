package main

import (
	"flag"
	"fmt"
	"go-mongo-proxy/proxy"
	"os"
)
import l4g "github.com/moovweb/log4go"

var log l4g.Logger

func main() {
	var config_path, password string

	flag.StringVar(&config_path, "c", "", "proxy config file")
	flag.StringVar(&password, "p", "", "admin password to connect mongo")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s -c <config_file> -p <admin password>\n", os.Args[0])
		os.Exit(-1)
	}

	flag.Parse()
	if flag.NArg() < 2 && (config_path == "" || password == "") {
		flag.Usage()
	}

	conf := load_config(config_path)
	conf.MONGODB.PASS = password

	log = make(l4g.Logger)
	log_init(log, conf)
	defer log_fini(log)

	proxy.Start(conf, log)
}
