package tunnel

import (
	"fmt"
	"os"
	"proxy_tunnel/logger"
	"syscall"
)

var runTunnel *Tunnel
var fdTunnelConn = make(map[int]*TunnelConn)
var epollFd int

func Stop() {
	logger.Log(logger.INFO, "Stop proxy server")
	if runTunnel != nil {
		syscall.Close(runTunnel.LFd)
		for _, tc := range fdTunnelConn {
			syscall.Close(tc.EFd)
			syscall.Close(tc.IFd)
		}
	}
}

func Run(t *Tunnel) {
	var err error
	epollFd, err = syscall.EpollCreate(1024)
	if err != nil {
		logger.Log(logger.ERR, "Create epoll fd error [%s]", err)
		os.Exit(-2)
	}

	for _, step := range initStep {
		err = step.Action(t)
		if err != nil {
			fmt.Fprintf(os.Stderr, step.ErrFmt, err)
			os.Exit(-2)
		}
	}
	runTunnel = t

	events := make([]syscall.EpollEvent, 10, 10)
	for {
		en, err := syscall.EpollWait(epollFd, events, 1000)
		if err != nil {
			logger.Log(logger.ERR, "Wail epoll fd error [%s]", err)
			os.Exit(-2)
		}
		for i := 0; i < en; i++ {
			ee := events[i]
			if runTunnel.LFd == int(ee.Fd) {
				runTunnel.newConn()
				continue
			}
			tc, ok := fdTunnelConn[int(ee.Fd)]
			if !ok {
				continue
			}
			if ee.Events&syscall.EPOLLIN != 0 {
				tc.handleIn(int(ee.Fd))
			}
			if ee.Events&syscall.EPOLLOUT != 0 {
				tc.handleOut(int(ee.Fd))
			}
			if ee.Events&syscall.EPOLLHUP != 0 {
				tc.shutdown()
			}
		}
	}
}
