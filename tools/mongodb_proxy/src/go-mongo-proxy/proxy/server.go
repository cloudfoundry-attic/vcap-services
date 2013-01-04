package proxy

import (
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
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
var sighnd chan os.Signal

func startProxyServer(conf *ProxyConfig) error {
	proxyaddrstr := conf.HOST + ":" + conf.PORT
	mongoaddrstr := conf.MONGODB.HOST + ":" + conf.MONGODB.PORT

	proxyfd, err := net.Listen("tcp", proxyaddrstr)
	if err != nil {
		logger.Error("TCP server listen error: [%v].", err)
		return err
	}

	filter := NewFilter(&conf.FILTER, &conf.MONGODB)
	if filter.FilterEnabled() {
		go filter.StartStorageMonitor()
	}

	manager := NewSessionManager()

	setupSignal()

	logger.Info("Start proxy server.")

	for {
		select {
		case <-sighnd:
			goto Exit
		default:
		}

		// Golang does not provide 'Timeout' IO function, so we
		// make it on our own.
		clientconn, err := asyncAcceptTCP(proxyfd, time.Second)
		if err == ErrTimeout {
			continue
		} else if err != nil {
			logger.Error("TCP server accept error: [%v].", err)
			continue
		}

		// If we cannot connect to backend mongodb instance within 5 seconds,
		// then we disconnect with client.
		serverconn, err := net.DialTimeout("tcp", mongoaddrstr, time.Second*5)
		if err != nil {
			logger.Error("TCP connect error: [%v].", err)
			clientconn.Close()
			continue
		}

		session := manager.NewSession(clientconn, serverconn, filter)
		go session.Process()
	}

Exit:
	logger.Info("Stop proxy server.")
	manager.WaitAllFinish()
	filter.WaitForFinish()
	return nil
}

type tcpconn struct {
	err error
	fd  net.Conn
}

var asynctcpconn chan tcpconn

func asyncAcceptTCP(serverfd net.Listener, timeout time.Duration) (net.Conn, error) {
	t := time.NewTimer(timeout)
	defer t.Stop()

	if asynctcpconn == nil {
		asynctcpconn = make(chan tcpconn, 1)
		go func() {
			connfd, err := serverfd.Accept()
			if err != nil {
				asynctcpconn <- tcpconn{err, nil}
			} else {
				asynctcpconn <- tcpconn{nil, connfd}
			}
		}()
	}

	select {
	case p := <-asynctcpconn:
		asynctcpconn = nil
		return p.fd, p.err
	case <-t.C:
		return nil, ErrTimeout
	}
	panic("Oops, unreachable")
}

func setupSignal() {
	sighnd = make(chan os.Signal, 1)
	signal.Notify(sighnd, syscall.SIGTERM)
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
