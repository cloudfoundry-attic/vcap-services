package main

import (
	"go-mongo-proxy/proxy"
	"os"
	"path/filepath"
)
import l4g "github.com/moovweb/log4go"

func log_init(log l4g.Logger, conf *proxy.ProxyConfig) {
	log_level := l4g.INFO
	switch conf.LOGGING.LEVEL {
	case "debug":
		log_level = l4g.DEBUG
	case "info":
		log_level = l4g.INFO
	case "warning":
		log_level = l4g.WARNING
	case "error":
		log_level = l4g.ERROR
	case "critical":
		log_level = l4g.CRITICAL
	}
	log_path := conf.LOGGING.PATH
	os.MkdirAll(filepath.Dir(log_path), 0755)
	log.AddFilter("file", log_level, l4g.NewFileLogWriter(log_path, true))
}

func log_fini(log l4g.Logger) {
	log.Close()
}
