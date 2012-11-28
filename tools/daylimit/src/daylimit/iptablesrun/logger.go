package iptablesrun

import (
	steno "github.com/cloudfoundry/gosteno"
	"os"
)

var obj steno.Logger

func Logger() steno.Logger {
	return obj
}

func InitLog(logFile string) {
	c := &steno.Config{
		Level:     steno.LOG_INFO,
		Codec:     steno.NewJsonCodec(),
		EnableLOC: true,
	}
	if logFile == "" {
		c.Sinks = []steno.Sink{steno.NewIOSink(os.Stdout)}
	} else {
		c.Sinks = []steno.Sink{steno.NewFileSink(logFile)}
	}
	steno.Init(c)
	obj = steno.NewLogger("")
}
