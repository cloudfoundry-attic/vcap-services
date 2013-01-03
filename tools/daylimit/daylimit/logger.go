package daylimit

import (
	steno "github.com/cloudfoundry/gosteno"
	"os"
)

var logger steno.Logger

func Logger() steno.Logger {
	if logger == nil {
		panic("Logger is used before init")
	}
	return logger
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
	logger = steno.NewLogger("")
}
