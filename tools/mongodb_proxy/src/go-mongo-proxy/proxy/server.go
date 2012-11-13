package proxy

import (
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

import l4g "github.com/moovweb/log4go"

type ConnectionInfo struct {
	HOST   string
	PORT   string
	DBNAME string
	USER   string
	PASS   string
}

type ProxyConfig struct {
	HOST string
	PORT string

	MONGODB ConnectionInfo

	FILTER struct {
		FS_RESERVED_BLOCKS float64
		THRESHOLD          float64
		ENABLED            bool
	}

	LOGGING struct {
		LEVEL string
		PATH  string
	}
}

var logger l4g.Logger

type ProxyServer struct {
	max_listen_fds int
	timeout        int  // milliseconds
	quit           bool // quit the whole process or not

	proxy_endpoint syscall.SockaddrInet4
	mongo_endpoint syscall.SockaddrUnix

	epoll_fd int
	events   []syscall.EpollEvent

	sighnd chan os.Signal
}

func StartProxyServer(conf *ProxyConfig, proxy_log l4g.Logger) (err error) {
	logger = proxy_log

	var filter *IOFilterProtocol
	var netio *NetIOManager

	proxy := &ProxyServer{
		max_listen_fds: 1024,
		timeout:        1000,
		quit:           false,
		epoll_fd:       -1,
		events:         make([]syscall.EpollEvent, 100),
		sighnd:         make(chan os.Signal, 1),
	}

	if !parse_config(proxy, conf) {
		logger.Error("Failed to initialize proxy server.")
		goto Error
	}

	filter = NewIOFilterProtocol(conf)
	if filter == nil {
		logger.Error("Failed to initialize filter protocol.")
		goto Error
	} else {
		if filter.FilterEnabled() {
			go filter.MonitDiskUsage()
		}
	}

	netio = NewNetIOManager()
	netio.SetFilter(filter)

	proxy.epoll_fd, err = syscall.EpollCreate(proxy.max_listen_fds)
	if err != nil {
		logger.Critical("Failed to initialize epoll listener [%s].", err)
		goto Cleanup
	}
	netio.SetEpollFd(proxy.epoll_fd)

	err = netio.ProxyNetListen(&proxy.proxy_endpoint)
	if err != nil {
		logger.Critical("Failed to initialize server listener [%s].", err)
		goto Cleanup
	}

	setup_sighnd(proxy)

	logger.Info("Mongodb proxy server start.")

	for {
		wait_signal(proxy, syscall.SIGTERM)

		if proxy.quit {
			break
		}

		nfds, err := syscall.EpollWait(proxy.epoll_fd, proxy.events,
			proxy.timeout)
		if err != nil {
			logger.Critical("Failed to do epoll wait [%s].", err)
			break
		}

		for i := 0; i < nfds; i++ {
			fd := int(proxy.events[i].Fd)

			if netio.ProxyNetIsProxyServer(fd) {
				clientinfo, err := netio.ProxyNetAccept(&proxy.mongo_endpoint)
				if err != nil {
					logger.Critical("Failed to establish bridge between mongo client and server [%s].", err)
				} else {
					ipaddr, port := parse_sockaddr(clientinfo)
					logger.Debug("Succeed to establish bridge for client [%s:%d].", ipaddr, port)
				}
			} else {
				event := proxy.events[i].Events

				if event&syscall.EPOLLIN != 0 {
					errno := netio.ProxyNetRecv(fd)

					switch errno {
					case READ_ERROR:
						sa := netio.ProxyNetConnInfo(fd)
						if sa != nil {
							ipaddr, port := parse_sockaddr(sa)
							logger.Error("Failed to read data from [%s:%d].", ipaddr, port)
						}
					case SESSION_EOF:
						sa := netio.ProxyNetConnInfo(fd)
						if sa != nil {
							ipaddr, port := parse_sockaddr(sa)
							logger.Debug("One side [%s:%d] close the session.", ipaddr, port)
						}
					case UNKNOWN_ERROR:
						sa := netio.ProxyNetConnInfo(fd)
						if sa != nil {
							ipaddr, port := parse_sockaddr(sa)
							logger.Debug("Unknown error during read happened at [%s:%d].", ipaddr, port)
						}
					}

					if errno != NO_ERROR {
						netio.ProxyNetClosePeers(fd)
					}
				}

				if event&syscall.EPOLLOUT != 0 {
					errno := netio.ProxyNetSend(fd)

					switch errno {
					case WRITE_ERROR:
						sa := netio.ProxyNetConnInfo(fd)
						if sa != nil {
							ipaddr, port := parse_sockaddr(sa)
							logger.Error("Failed to write data to [%s:%d]", ipaddr, port)
						}
					case FILTER_BLOCK:
						sa := netio.ProxyNetConnInfo(fd)
						if sa != nil {
							ipaddr, port := parse_sockaddr(sa)
							logger.Error("Filter block request from client [%s:%d].", ipaddr, port)
						}
					case UNKNOWN_ERROR:
						sa := netio.ProxyNetConnInfo(fd)
						if sa != nil {
							ipaddr, port := parse_sockaddr(sa)
							logger.Debug("Unknown error during write happened at [%s:%d].", ipaddr, port)
						}
					}

					if errno != NO_ERROR {
						netio.ProxyNetClosePeers(fd)
					}
				}

				if event&syscall.EPOLLRDHUP != 0 {
					sa := netio.ProxyNetConnInfo(fd)
					if sa != nil {
						ipaddr, port := parse_sockaddr(sa)
						logger.Debug("shutdown connection with [%s:%d].", ipaddr, port)
						netio.ProxyNetClosePeers(fd)
					}
				}

				if event&syscall.EPOLLHUP != 0 {
					sa := netio.ProxyNetConnInfo(fd)
					if sa != nil {
						ipaddr, port := parse_sockaddr(sa)
						logger.Debug("shutdown connection with [%s:%d].", ipaddr, port)
						netio.ProxyNetClosePeers(fd)
					}
				}
			}
		}
	}

Cleanup:
	netio.DestroyNetIO()
Error:
	logger.Info("Mongodb proxy server quit.")
	logger.Close()
	return err
}

/******************************************/
/*                                        */
/*       Internal Support Routines        */
/*                                        */
/******************************************/
func parse_config(proxy *ProxyServer, conf *ProxyConfig) (retval bool) {
	proxy_ipaddr := net.ParseIP(conf.HOST)
	if proxy_ipaddr == nil {
		logger.Error("Proxy ipaddr format error.")
		return false
	}

	proxy_port, err := strconv.Atoi(conf.PORT)
	if err != nil {
		logger.Error(err)
		return false
	}

	// TODO: need a protable way not hard code to parse ip address
	proxy.proxy_endpoint = syscall.SockaddrInet4{Port: proxy_port,
		Addr: [4]byte{proxy_ipaddr[12],
			proxy_ipaddr[13],
			proxy_ipaddr[14],
			proxy_ipaddr[15]}}

	// the channel between proxy and mongo server is shipped on Unix Socket
	proxy.mongo_endpoint = syscall.SockaddrUnix{
		Name: "/tmp/mongodb-27017.sock",
	}
	return true
}

func setup_sighnd(proxy *ProxyServer) (c chan os.Signal) {
	signal.Notify(proxy.sighnd, syscall.SIGTERM)
	return proxy.sighnd
}

func wait_signal(proxy *ProxyServer, sig os.Signal) {
	select {
	case s := <-proxy.sighnd:
		if s == sig {
			proxy.quit = true
		}
	default:
		return
	}
}

func parse_sockaddr(sa syscall.Sockaddr) (ipaddr net.IP, port int) {
	switch sa := sa.(type) {
	case *syscall.SockaddrInet4:
		return net.IPv4(sa.Addr[0], sa.Addr[1], sa.Addr[2], sa.Addr[3]), sa.Port
	}
	return net.IPv4(0, 0, 0, 0), 0
}
