package main

import (
	"flag"
	"fmt"
	steno "github.com/cloudfoundry/gosteno"
	"go-mongo-proxy/proxy"
	"os"
)

var log steno.Logger

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

	setup_steno(conf)
	log = steno.NewLogger("mongodb_proxy")

	proxy.Start(conf, log)
}
