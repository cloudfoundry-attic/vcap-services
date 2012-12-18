package proxy

import (
	"flag"
	"net"
)
import l4g "github.com/moovweb/log4go"

type ProxyConfig struct {
	HOST string
	PORT string

	FILTER FilterConfig

	MONGODB ConnectionInfo

	LOGGING struct {
		LEVEL string
		PATH  string
	}
}

var logger l4g.Logger

func startProxyServer(conf *ProxyConfig) error {
	proxyaddrstr := flag.String("proxy listen address", conf.HOST+":"+conf.PORT, "host:port")
	mongoaddrstr := flag.String("mongo listen address", conf.MONGODB.HOST+":"+conf.MONGODB.PORT, "host:port")

	proxyaddr, err := net.ResolveTCPAddr("tcp", *proxyaddrstr)
	if err != nil {
		logger.Error("TCP addr resolve error: [%v].", err)
		return err
	}

	mongoaddr, err := net.ResolveTCPAddr("tcp", *mongoaddrstr)
	if err != nil {
		logger.Error("TCP addr resolve error: [%v].", err)
		return err
	}

	proxyfd, err := net.ListenTCP("tcp", proxyaddr)
	if err != nil {
		logger.Error("TCP server listen error: [%v].", err)
		return err
	}

	filter := NewFilter(&conf.FILTER, &conf.MONGODB)
	if filter.FilterEnabled() {
		go filter.StorageMonitor()
	}

	logger.Info("Start proxy server.")

	for {
		clientconn, err := proxyfd.AcceptTCP()
		if err != nil {
			logger.Error("TCP server accept error: [%v].", err)
			continue
		}

		serverconn, err := net.DialTCP("tcp", nil, mongoaddr)
		if err != nil {
			logger.Error("TCP connect error: [%v].", err)
			clientconn.Close()
			continue
		}

		session := NewSession(clientconn, serverconn, filter)
		go session.Process()
	}

	logger.Info("Stop proxy server.")
	return nil
}

func Start(conf *ProxyConfig, log l4g.Logger) error {
	if log == nil {
		logger = make(l4g.Logger)
		logger.AddFilter("stdout", l4g.DEBUG, l4g.NewConsoleLogWriter())
	} else {
		logger = log
	}
	return startProxyServer(conf)
}
