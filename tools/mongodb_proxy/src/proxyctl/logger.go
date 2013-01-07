package main

import (
	steno "github.com/cloudfoundry/gosteno"
	"go-mongo-proxy/proxy"
	"os"
	"path/filepath"
)

func setup_steno(conf *proxy.ProxyConfig) {
	level, err := steno.GetLogLevel(conf.LOGGING.LEVEL)
	if err != nil {
		panic(err)
	}

	log_path := conf.LOGGING.PATH
	if log_path != "" {
		os.MkdirAll(filepath.Dir(log_path), 0755)
	}

	sinks := make([]steno.Sink, 0)
	if log_path != "" {
		sinks = append(sinks, steno.NewFileSink(log_path))
	} else {
		sinks = append(sinks, steno.NewIOSink(os.Stdout))
	}

	stenoConfig := &steno.Config{
		Sinks: sinks,
		Codec: steno.NewJsonCodec(),
		Level: level,
	}

	steno.Init(stenoConfig)
}
